FROM mcr.microsoft.com/devcontainers/python:3.13

# Rootless podman runs the build in a user namespace, where apt's privilege-dropped
# _apt sandbox user can't write temp files to /tmp during signature verification.
# Run apt as root instead so feature installs (d2, etc.) succeed.
RUN echo 'APT::Sandbox::User "root";' > /etc/apt/apt.conf.d/99-disable-sandbox

# azcopy has no devcontainer feature, so install the pinned release directly.
# --no-same-owner keeps tar from chowning to a UID the build's user namespace can't map.
RUN curl -fsSL -o /tmp/azcopy.tar.gz \
        https://github.com/Azure/azure-storage-azcopy/releases/download/v10.34.1/azcopy_linux_amd64_10.34.1.tar.gz \
    && echo "a0021809e53f2b40705509626b8d44f2312fa8ef39deeb941fcffd35012bf6d1  /tmp/azcopy.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/azcopy.tar.gz --no-same-owner --strip-components=1 -C /usr/local/bin \
        azcopy_linux_amd64_10.34.1/azcopy \
    && chmod 755 /usr/local/bin/azcopy \
    && rm /tmp/azcopy.tar.gz
