FROM liangduanqi/ta-lib:v1.2

COPY . /work

WORKDIR /work
#RUN mkdir ~/.pip && cd ~/.pip/  && echo "[global] \ntrusted-host =  pypi.douban.com \nindex-url = http://pypi.douban.com/simple" >  pip.conf
#RUN env LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib" pip install mysqlclient
#RUN pip install -r requirements.txt -i  https://pypi.tuna.tsinghua.edu.cn/simple
#RUN pip install -r requirements.txt -i https://pypi.mirrors.ustc.edu.cn/simple/
#RUN sh install_nodejs.sh
RUN pip install --upgrade pip -i  https://pypi.doubanio.com/simple/
RUN pip install git+http://liangduanqi:agucha2020%2A%2A@139.9.134.29/liangduanqi/sycm.git#egg=sycm
RUN pip install -r requirements.txt -i https://pypi.doubanio.com/simple/
#RUN sh install.sh
RUN echo "packpage has installed"
#RUN cp prod_entrypoint.sh /usr/bin/ && chmod +x /usr/bin/prod_entrypoint.sh

#ENTRYPOINT ["prod_entrypoint.sh"]
EXPOSE 8000
EXPOSE 5555




#CMD ["python","manage.py","runserver","0.0.0.0:8000"]
