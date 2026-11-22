# Diagnostic only: build shopping-sessions-builder:v5-mixed with --target build first.
FROM shopping-sessions-builder:v5-mixed
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgoogle-perftools-dev google-perftools file && rm -rf /var/lib/apt/lists/*
RUN cmake -S /src -B /src/build-profile -DCMAKE_BUILD_TYPE=Release -DSHOP_CPU_PROFILE=ON -DCMAKE_EXE_LINKER_FLAGS=-Wl,--no-as-needed && cmake --build /src/build-profile -j8
USER 1000:1000
ENTRYPOINT ["/src/build-profile/shopping"]
