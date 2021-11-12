FROM openjdk:12.0.1-jdk-oraclelinux7

ARG installfile=file_not_set

ADD dbloadgen-entry.sh /

# automatically unpacks tar.gz
ADD  $installfile /

WORKDIR /dbloadgen

RUN chmod +x /dbloadgen-entry.sh

ENTRYPOINT ["/dbloadgen-entry.sh"]

