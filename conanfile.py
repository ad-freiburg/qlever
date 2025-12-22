import os
from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.env import VirtualRunEnv

class QLeverRecipe(ConanFile):
    description = ""
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeToolchain", "CMakeDeps"

    def build(self):
        pass

    def export(self):
        pass

    def build_requirements(self):
        if self.settings.arch == "wasm64":
           self.tool_requires("emsdk/3.1.73")
           self.tool_requires("cmake/4.2.0")

    def requirements(self):
        self.requires("boost/1.81.0")
        self.requires("icu/76.1")
        self.requires("openssl/3.1.1")
        self.requires("zstd/1.5.5")
        # The jemalloc recipe for Conan2 is currently broken, uncomment this line as soon as this is fixed.
        #jemalloc/5.3.0

    def layout(self):
        if self.settings.arch == "wasm64":
           cmake_layout(self)

    def package_info(self):
        pass
