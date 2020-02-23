docker-compose -f itest-docker-compose.yml down --remove-orphans --volumes
sudo rm -rf /Users/demo/volumes/test/gpfdist
docker-compose -f itest-docker-compose.yml up --build
docker-compose -f itest-docker-compose.yml down --remove-orphans --volumes
