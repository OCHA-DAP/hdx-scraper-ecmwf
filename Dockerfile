FROM public.ecr.aws/unocha/python:3.13-stable

WORKDIR /srv

RUN --mount=type=bind,source=requirements.txt,target=requirements.txt \
    apk add --no-cache \
    aws-cli \
    gdal-driver-parquet \
    gdal-tools && \
    apk add --no-cache --virtual .build-deps \
    build-base \
    gdal-dev \
    llvm15-dev && \
    LLVM_CONFIG=/usr/bin/llvm-config-15 \
    pip install --no-cache-dir -r requirements.txt && \
    apk del .build-deps && \
    rm -rf /var/lib/apk/*

COPY src ./

CMD "python3 run.py"
