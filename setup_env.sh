conda create -y -n qlever
conda activate qlever

conda install -y -c conda-forge python=3.8
conda install -y -c conda-forge cmake
conda install -y -c conda-forge gcc=14.3.0
conda install -y -c conda-forge gxx=14.3.0
conda install -y -c conda-forge gdb
conda install -y -c conda-forge ninja
conda install -y -c conda-forge libcxx
conda install -y -c conda-forge glib
conda install -y -c conda-forge gxx_linux-64
conda install -y -c conda-forge ninja
conda install -y -c anaconda zlib
conda install -y -c conda-forge jemalloc
conda install -y -c conda-forge ld_impl_linux-64
# conda install -y -c conda-forge boost==1.83.0
pip install conan

conan profile detect --name qlever
CONAN_PROFILE_FILE=$HOME/.conan2/profiles/qlever.txt

echo "[buildenv]" >> $CONAN_PROFILE_FILE
echo "CC=gcc" >> $CONAN_PROFILE_FILE
echo "CXX=g++" >> $CONAN_PROFILE_FILE   
echo "LD=x86_64-conda-linux-gnu-ld" >> $CONAN_PROFILE_FILE  


echo "[conf]" >> $CONAN_PROFILE_FILE
echo "tools.cmake.cmaketoolchain:generator=Ninja" >> $CONAN_PROFILE_FILE

conan install . -if build --build=missing