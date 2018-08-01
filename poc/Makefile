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
		-w /zipline --name $(NAME) $(REPO) bash