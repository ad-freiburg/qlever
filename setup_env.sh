conda create -y -n qlever
conda activate qlever

conda install -y -c conda-forge python=3.8
conda install -y -c conda-forge boost==1.83.0
conda install -y -c conda-forge cmake
conda install -y -c conda-forge gcc
conda install -y -c conda-forge gxx
conda install -y -c conda-forge ninja
conda install -y -c conda-forge libcxx
conda install -y -c conda-forge glib
conda install -y -c conda-forge gxx_linux-64
conda install -y -c conda-forge ninja
conda install -y -c anaconda zlib
conda install -y -c conda-forge jemalloc
conda install -y -c conda-forge gdb
pip install conan