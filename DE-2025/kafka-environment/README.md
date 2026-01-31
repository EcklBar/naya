# 🐳 Docker & Docker Compose Cheatsheet

---

## 🐋 Docker CLI

### 🔧 Build & Run

```bash
docker build -t my-image .
docker run -it --rm my-image
docker run -d -p 8080:80 --name my-container my-image


⸻

📦 Manage Images & Containers

docker ps           # Running containers
docker ps -a        # All containers
docker images       # List images
docker stop <id>    # Stop container
docker rm <id>      # Remove container
docker rmi <img>    # Remove image


⸻

🔍 Logs, Exec, Inspect

docker logs -f <container>
docker exec -it <container> bash
docker inspect <container>


⸻

🧹 Cleanup

docker stop $(docker ps -q)
docker rm $(docker ps -aq)
docker rmi $(docker images -q)
docker system prune -af


⸻

📦 Docker Compose

🚀 Up & Down

docker-compose up                  # Start all
docker-compose up -d               # Detached
docker-compose down                # Stop and remove
docker-compose down -v             # Also remove volumes

🔁 Rebuild, Restart, Logs

docker-compose build
docker-compose up --build
docker-compose restart <service>
docker-compose logs -f

🐚 Exec

docker-compose exec <service> bash


⸻

🗂️ Volumes

🛠 CLI

docker volume create my-volume
docker volume ls
docker volume inspect my-volume
docker volume rm my-volume

📦 In docker-compose.yml

services:
  db:
    image: postgres
    volumes:
      - db-data:/var/lib/postgresql/data

volumes:
  db-data:


⸻

🌐 Networks

🛠 CLI

docker network ls
docker network create my-net
docker network inspect my-net
docker network rm my-net

🌐 In docker-compose.yml

services:
  app:
    image: my-app
    networks:
      - backend

  db:
    image: mongo
    networks:
      - backend

networks:
  backend:
    driver: bridge


⸻

📋 Utilities

docker-compose ps
docker-compose config     # Validate config