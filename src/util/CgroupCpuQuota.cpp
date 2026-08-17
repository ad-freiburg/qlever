//   Copyright 2026, University of Freiburg,
//   Chair of Algorithms and Data Structures.

#include "util/CgroupCpuQuota.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <thread>

#include "util/Log.h"

#ifdef __linux__
#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace ad_utility {

namespace {

// The cgroup v2 bandwidth period. 100ms is the kernel default.
constexpr int64_t cpuPeriodUsec = 100'000;

#ifdef __linux__
// Write `content` to `file` with a single plain `write` syscall. Returns
// `0` on success and the `errno` of the failing syscall otherwise. Cgroup
// control files must not be written via buffered streams: `std::ofstream`
// may defer the failing `write` to the close and then swallow the error,
// making failures undetectable.
int writeToCgroupFile(const std::filesystem::path& file,
                      const std::string& content) {
  int fd = ::open(file.c_str(), O_WRONLY);
  if (fd < 0) {
    return errno;
  }
  ssize_t written;
  do {
    written = ::write(fd, content.data(), content.size());
  } while (written < 0 && errno == EINTR);
  int result = written == static_cast<ssize_t>(content.size()) ? 0 : errno;
  ::close(fd);
  return result;
}

pid_t currentTid() { return static_cast<pid_t>(::syscall(SYS_gettid)); }

// The server's own cgroup directory according to `/proc/self/cgroup`
// (format `0::<path>` for cgroup v2).
std::optional<std::filesystem::path> ownCgroupDir() {
  std::ifstream stream{"/proc/self/cgroup"};
  std::string line;
  while (std::getline(stream, line)) {
    if (line.starts_with("0::")) {
      return std::filesystem::path{"/sys/fs/cgroup"} /
             std::filesystem::path{line.substr(3)}.relative_path();
    }
  }
  return std::nullopt;
}
#endif

}  // namespace

// _____________________________________________________________________________
CgroupCpuQuota::CgroupCpuQuota(std::filesystem::path dir,
                               std::filesystem::path parentThreadsFile)
    : dir_{std::move(dir)}, parentThreadsFile_{std::move(parentThreadsFile)} {}

// _____________________________________________________________________________
CgroupCpuQuota::~CgroupCpuQuota() {
#ifdef __linux__
  // Worker threads normally have exited or left already. If the group is
  // still busy, wait briefly, then give up and leak the group (it becomes
  // removable once its last thread exits and does no harm until then).
  std::error_code ec;
  for (size_t i = 0; i < 20; ++i) {
    std::filesystem::remove(dir_, ec);
    if (!ec) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  AD_LOG_WARN << "Could not remove query cgroup " << dir_ << " ("
              << ec.message() << ")" << std::endl;
#endif
}

// _____________________________________________________________________________
void CgroupCpuQuota::enterCurrentThread() const {
#ifdef __linux__
  pid_t tid = currentTid();
  int error = writeToCgroupFile(dir_ / "cgroup.threads", std::to_string(tid));
  if (error != 0) {
    AD_LOG_WARN << "Failed to move thread " << tid << " into query cgroup "
                << dir_ << " (" << std::strerror(error) << ")" << std::endl;
  }
#endif
}

// _____________________________________________________________________________
void CgroupCpuQuota::leaveCurrentThread() const {
#ifdef __linux__
  pid_t tid = currentTid();
  int error = writeToCgroupFile(parentThreadsFile_, std::to_string(tid));
  if (error != 0) {
    // A thread that stays behind in the query cgroup keeps its quota for
    // unrelated work and prevents the removal of the cgroup, so this must
    // not fail silently.
    AD_LOG_WARN << "Failed to move thread " << tid
                << " back out of query cgroup " << dir_ << " ("
                << std::strerror(error) << ")" << std::endl;
  }
#endif
}

// _____________________________________________________________________________
std::optional<uint64_t> CgroupCpuQuota::usageUsec() const {
#ifdef __linux__
  std::ifstream stream{dir_ / "cpu.stat"};
  std::string key;
  uint64_t value;
  while (stream >> key >> value) {
    if (key == "usage_usec") {
      return value;
    }
  }
#endif
  return std::nullopt;
}

// _____________________________________________________________________________
CgroupCpuQuotaManager& CgroupCpuQuotaManager::getInstance() {
  static CgroupCpuQuotaManager instance;
  return instance;
}

// _____________________________________________________________________________
void CgroupCpuQuotaManager::initialize() {
  if (initialized_) {
    return;
  }
  initialized_ = true;
#ifdef __linux__
  auto base = ownCgroupDir();
  if (!base.has_value()) {
    AD_LOG_INFO << "Per-query CPU quotas disabled (no cgroup v2 found)"
                << std::endl;
    return;
  }
  baseDir_ = base.value();

  // Probe the full lifecycle of a query cgroup once: create a threaded
  // subgroup, enable the cpu controller for subgroups, set a quota, move the
  // current thread in and out, remove the subgroup. Only if all of this
  // works is the feature enabled.
  auto probeDir = baseDir_ / "ql_probe";
  std::error_code ec;
  // A stale probe directory from an unclean shutdown would make the
  // creation fail and thereby permanently disable the feature, so remove
  // it first (the removal fails harmlessly if there is nothing to remove;
  // note that cgroupfs only supports plain `rmdir` of empty leaves).
  std::filesystem::remove(probeDir, ec);
  std::filesystem::create_directory(probeDir, ec);
  bool ok = !ec;
  ok = ok && writeToCgroupFile(probeDir / "cgroup.type", "threaded") == 0;
  ok =
      ok && writeToCgroupFile(baseDir_ / "cgroup.subtree_control", "+cpu") == 0;
  ok = ok && writeToCgroupFile(probeDir / "cpu.max",
                               std::to_string(cpuPeriodUsec) + " " +
                                   std::to_string(cpuPeriodUsec)) == 0;
  ok = ok && writeToCgroupFile(probeDir / "cgroup.threads",
                               std::to_string(currentTid())) == 0;
  ok = ok && writeToCgroupFile(baseDir_ / "cgroup.threads",
                               std::to_string(currentTid())) == 0;
  std::filesystem::remove(probeDir, ec);
  supported_ = ok;
  if (supported_) {
    AD_LOG_INFO << "Per-query CPU quotas available (cgroup " << baseDir_ << ")"
                << std::endl;
  } else {
    AD_LOG_INFO << "Per-query CPU quotas disabled (cgroup " << baseDir_
                << " is not writable or does not support threaded subgroups;"
                << " run the server under e.g. `systemd-run -p Delegate=yes`"
                << " to enable them)" << std::endl;
  }
#else
  AD_LOG_INFO << "Per-query CPU quotas are only supported on Linux"
              << std::endl;
#endif
}

// _____________________________________________________________________________
std::shared_ptr<CgroupCpuQuota> CgroupCpuQuotaManager::createQuota(
    double cores) {
  if (!supported_ || cores <= 0) {
    return nullptr;
  }
#ifdef __linux__
  // The PID in the name avoids collisions with cgroups leaked by a
  // previous server process in the same scope (the counter restarts at
  // zero) and attributes leftovers to their process when debugging.
  auto dir = baseDir_ / ("ql_query_" + std::to_string(::getpid()) + "_" +
                         std::to_string(nextId_.fetch_add(1)));
  std::error_code ec;
  std::filesystem::create_directory(dir, ec);
  if (ec) {
    return nullptr;
  }
  auto quotaUsec = static_cast<int64_t>(cores * cpuPeriodUsec);
  bool ok = writeToCgroupFile(dir / "cgroup.type", "threaded") == 0 &&
            writeToCgroupFile(dir / "cpu.max",
                              std::to_string(quotaUsec) + " " +
                                  std::to_string(cpuPeriodUsec)) == 0;
  if (!ok) {
    std::filesystem::remove(dir, ec);
    return nullptr;
  }
  return std::shared_ptr<CgroupCpuQuota>{
      new CgroupCpuQuota{std::move(dir), baseDir_ / "cgroup.threads"}};
#else
  return nullptr;
#endif
}

}  // namespace ad_utility
