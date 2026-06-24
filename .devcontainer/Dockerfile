FROM mcr.microsoft.com/devcontainers/python:3.11

# Rootless podman runs the build in a user namespace, where apt's privilege-dropped
# _apt sandbox user can't write temp files to /tmp during signature verification.
# Run apt as root instead so feature installs (d2, etc.) succeed.
RUN echo 'APT::Sandbox::User "root";' > /etc/apt/apt.conf.d/99-disable-sandbox
