FROM httpd:2.4

RUN apt-get update && apt-get install -y ca-certificates libapache2-mod-auth-openidc

RUN echo "Include /usr/local/apache2/conf/extra/httpd-metax.conf" >> /usr/local/apache2/conf/httpd.conf

EXPOSE 8080
