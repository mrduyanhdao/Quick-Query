FROM  amancevice/pandas:1.4.3-slim
WORKDIR /app
RUN  pip install --upgrade pip && pip install pyyaml && pip install google-cloud && pip install Jinja2
COPY . /app
VOLUME ["/app/Result"]
VOLUME ["/app/Template"]
EXPOSE 80
CMD ["python", "quick_analysis.py"]

