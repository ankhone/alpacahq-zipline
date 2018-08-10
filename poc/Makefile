REPO=zipline-poc
NAME=$(REPO)

build:
	docker build -t $(REPO) .

run:
	docker rm -f $(NAME) || echo
	docker run -it -v $(CURDIR)/..:/zipline \
		-e APCA_API_KEY_ID \
		-e APCA_API_SECRET_KEY \
		-e APCA_API_BASE_URL \
		-p 8888:8888 \
		-v $(CURDIR)/.zipline:/root/.zipline \
		-w /zipline --name $(NAME) $(REPO) bash