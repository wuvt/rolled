FROM docker.io/library/nginx:alpine

RUN rm -rf /usr/share/nginx/html/*
COPY . /usr/share/nginx/html
RUN chown -R nginx:nginx /usr/share/nginx/html

COPY default.conf /etc/nginx/conf.d/default.conf
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 80
ENTRYPOINT ["/entrypoint.sh"]
CMD ["sh", "-c", "nginx -g 'daemon off;'"]