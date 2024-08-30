FROM rockylinux:9

ARG installfile=file_not_set

RUN dnf install -y java-11-openjdk java-11-openjdk-devel

ADD dbloadgen-entry.sh /

# automatically unpacks tar.gz
ADD  $installfile /

WORKDIR /dbloadgen

RUN chmod +x /dbloadgen-entry.sh

ENTRYPOINT ["/dbloadgen-entry.sh"]

