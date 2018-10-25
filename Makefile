default: build dockerize

build:
	mvn clean package -U -Dmaven.test.skip=true 

dockerize:
	docker build -f qa_system.docker -t git.project-hobbit.eu:4567/weekmo/asknow .
	
push: 	
	docker push git.project-hobbit.eu:4567/weekmo/asknow