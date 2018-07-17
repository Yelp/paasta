httpdrain:
	GOOS=linux GOARCH=amd64 go build

docker: httpdrain
	docker build -t httpdrain .

clean:
	rm httpdrain
	docker rmi httpdrain

run: docker
	docker run -p 3000:3000 httpdrain
