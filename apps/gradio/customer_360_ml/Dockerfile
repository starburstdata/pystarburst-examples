FROM python:3.11

RUN useradd -m -u 1000 user
USER user

# Set home to the user's home directory
ENV HOME=/home/user \
	PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY --chown=user . $HOME/code

RUN git config --global init.defaultBranch space

RUN --mount=type=secret,id=HOST,mode=0444,required=true
RUN --mount=type=secret,id=SB_USER,mode=0444,required=true
RUN --mount=type=secret,id=SB_PASS,mode=0444,required=true
RUN --mount=type=secret,id=OPEN_API_KEY,mode=0444,required=true 

RUN ls -R /run

CMD ["python", "app.py"]