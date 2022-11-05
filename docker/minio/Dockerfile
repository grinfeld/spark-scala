FROM minio/mc
RUN microdnf install wget

COPY ./docker/minio/setup.sh /setup.sh

CMD ["/setup.sh"]