FROM mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest

# Preserve the emulator image default startup behavior.
# Do not override working directory or COPY files into the emulator root,
# because the base image expects its own executable path.
EXPOSE 8081 10250
