//   Copyright 2026, University of Freiburg,
//   Chair of Algorithms and Data Structures.

#ifndef QLEVER_CGROUPCPUQUOTA_H
#define QLEVER_CGROUPCPUQUOTA_H

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

namespace ad_utility {

// Limit the CPU consumption of a single query via a dedicated cgroup v2
// subgroup with a `cpu.max` quota (Linux only, and only if the server's own
// cgroup is writable, e.g. when running under `systemd-run -p Delegate=yes`).
// The thread that computes a query enters the query's cgroup for the duration
// of each computation section. All worker threads that are spawned during such
// a section inherit the cgroup membership automatically, so the kernel
// enforces the quota for the whole query regardless of how many threads it
// uses. On unsupported systems all operations are no-ops.
//
// NOTE:
// Threads from pre-existing shared pools (in particular the OpenMP threads
// of the parallel sort) were created outside the query's cgroup and are
// therefore not covered by the quota. This is a known limitation.
class CgroupCpuQuota {
 public:
  ~CgroupCpuQuota();
  CgroupCpuQuota(const CgroupCpuQuota&) = delete;
  CgroupCpuQuota& operator=(const CgroupCpuQuota&) = delete;

  // Move the calling thread into (resp. out of) this query's cgroup.
  void enterCurrentThread() const;
  void leaveCurrentThread() const;

  // The total CPU time in microseconds consumed inside this cgroup so far,
  // from the kernel's own accounting (`cpu.stat`), or `nullopt` on error.
  std::optional<uint64_t> usageUsec() const;

 private:
  friend class CgroupCpuQuotaManager;
  CgroupCpuQuota(std::filesystem::path dir,
                 std::filesystem::path parentThreadsFile);
  std::filesystem::path dir_;
  std::filesystem::path parentThreadsFile_;
};

// RAII guard that makes the calling thread a member of the query's cgroup for
// the lifetime of the guard. A `nullptr` quota makes the guard a no-op.
class ScopedCgroupMembership {
 public:
  explicit ScopedCgroupMembership(const CgroupCpuQuota* quota) : quota_{quota} {
    if (quota_ != nullptr) {
      quota_->enterCurrentThread();
    }
  }
  ~ScopedCgroupMembership() {
    if (quota_ != nullptr) {
      quota_->leaveCurrentThread();
    }
  }
  ScopedCgroupMembership(const ScopedCgroupMembership&) = delete;
  ScopedCgroupMembership& operator=(const ScopedCgroupMembership&) = delete;

 private:
  const CgroupCpuQuota* quota_;
};

// Detects at startup whether per-query cgroups are possible and creates the
// per-query subgroups. A singleton because the underlying resource (the
// server's cgroup) is global to the process.
class CgroupCpuQuotaManager {
 public:
  static CgroupCpuQuotaManager& getInstance();

  // Probe the server's cgroup for writability and threaded-subgroup support.
  // Must be called once at server startup, before any threads are moved.
  // Idempotent. Logs the outcome.
  void initialize();

  bool isSupported() const { return supported_; }

  // Create a cgroup for one query with a quota of `cores` CPUs. Returns
  // `nullptr` if unsupported or `cores <= 0`.
  std::shared_ptr<CgroupCpuQuota> createQuota(double cores);

 private:
  CgroupCpuQuotaManager() = default;
  bool initialized_ = false;
  bool supported_ = false;
  std::filesystem::path baseDir_;
  std::atomic<uint64_t> nextId_ = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_CGROUPCPUQUOTA_H
