g++ client.cpp -std=c++11 -o oclient.out -lpthread
g++ server.cpp -std=c++11 -o oserver.out -lpthread
g++ slave.cpp -std=c++11 -lpthread -lsqlite3 -o oslave.out