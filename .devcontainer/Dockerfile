FROM ubuntu:24.04

# disable error checks for GPG
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LC_CTYPE=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN echo "Acquire::http::Pipeline-Depth 0;" > /etc/apt/apt.conf.d/99custom && \

    echo "Acquire::http::No-Cache true;" >> /etc/apt/apt.conf.d/99custom && \
    echo "Acquire::AllowInsecureRepositories true;" >> /etc/apt/apt.conf.d/99custom && \
    echo "Acquire::ForceIPv4 true;" >> /etc/apt/apt.conf.d/99custom && \
    echo "Acquire::BrokenProxy true;" >> /etc/apt/apt.conf.d/99custom

RUN apt-get update && apt-get install -y wget
RUN wget https://apt.kitware.com/kitware-archive.sh && chmod +x kitware-archive.sh && ./kitware-archive.sh

RUN apt -y --no-install-recommends install \
    build-essential \
    curl \
    git \
    python3 \
    python3-pip
RUN apt install cmake -y
# install icu and i18n
RUN apt install -y libicu-dev libjemalloc-dev pkg-config libssl-dev
RUN apt -y install libboost-all-dev
# install stdlibc++-dev
RUN apt -y install libstdc++6 libzstd-dev libbz2-dev libicu-dev

RUN apt -y install nodejs npm
RUN npm install -g @anthropic-ai/claude-code
RUN apt -y install ninja-build

RUN rm -rf /var/lib/apt/lists/*