FROM --platform=linux/amd64 python:3.12-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyarrow==21.0.0
WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
COPY tests/csv_check.cpp tests/csv_scan_check.cpp ./tests/
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/app && cmake --build build -j8 && cmake --install build && mkdir /opt/arrow && cp /usr/local/lib/python3.12/site-packages/pyarrow/libarrow.so.2100 /usr/local/lib/python3.12/site-packages/pyarrow/libparquet.so.2100 /opt/arrow/
FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/app/bin/shopping /usr/local/bin/shopping
COPY --from=build /opt/arrow /opt/arrow
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/shopping"]
