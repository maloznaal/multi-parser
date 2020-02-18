docker-compose -f itest-docker-compose.yml down --remove-orphans --volumes
sudo rm -rf /Users/demo/volumes/test
docker-compose -f itest-docker-compose.yml up --abort-on-container-exit --build
docker-compose -f itest-docker-compose.yml down --remove-orphans --volumes
