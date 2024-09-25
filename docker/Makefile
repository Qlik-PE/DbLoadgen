#
installfile=dbloadgen.tgz
image=attunitypm/dbloadgen
version=1.1
nocache=

build: image
	# Remove dangling images (intermittent images with tag <none>)
	yes | docker image prune > /dev/null

rebuild: nocache build

nocache: 
	# have docker rebuild images from scratch rather than using cache
	$(eval nocache := --no-cache)

image: 
	@echo "*** Creating tar bundle $(installfile)"
	cd ../release && tar czf $(installfile) ./dbloadgen 
	mv ../release/$(installfile) .
	@echo "*** Building dbloadgen image with $(nocache)"
	docker build $(nocache) --file Dockerfile --build-arg installfile=$(installfile) \
              -t $(image):$(version) .
	rm $(installfile)

# tags the specified image as "latest" stable
latest:
	docker tag $(image):$(version) $(image):latest

pushversion:
	docker push $(image):$(version)

pushlatest:
	docker push $(image):latest

push: pushversion pushlatest 

docker-release: rebuild latest push


