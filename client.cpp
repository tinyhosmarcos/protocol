// Client side C/C++ program to demonstrate Socket programming
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <thread>
#include <iostream>
#define PORT 1205
#define SER_PORT 1200

using namespace std;

void udpSocket(int &sock, struct sockaddr_in &serv_addr, int port, string ip="")
{
    sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(sock == -1){
	    printf("failed with inst socket: %s\n", strerror(errno));
	}
    serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);
	if(ip == "")
		serv_addr.sin_addr.s_addr = INADDR_ANY;
	else
		inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);
}

void threadedReceive(int server_fd){
	struct sockaddr_in client_addr;
	int addrlen = sizeof(client_addr);
	char buffer[1024] = {0};
	printf("receiving msgs on port %i\n", PORT);
	while(1){
		memset(buffer, 0, 1024);
		recvfrom(server_fd, (char *)buffer, 1000, 
				0, ( struct sockaddr *) &client_addr, 
				(socklen_t*)&addrlen);
		char *clientIp = inet_ntoa(client_addr.sin_addr);
		
		printf("%s says: %s\n",clientIp,buffer);
	}

}
int main(int argc, char const *argv[])
{
/*
prg.exe  <C> <node>  [<attribute-name> <attribute-value>...] [<relationship-node-name>]
prg.exe  <R> <node>  <node> <level 1..99> <include-attributes 1/0>
prg.exe  <U> <node>  <node N | attribute A> < <name-node><new-name> | <name|value><attribute-name><new-attribute-name | new-attribute-name> >
prg.exe  <D> <node>  <node N | attribute A> < <name-node> | <attribute-name> >
prg.exe  <Q> .... opcional + seria un plus
*/

	int sock = 0;
	struct sockaddr_in serv_addr;
	
	char *mesage1 = (char*)"1R081015151515";
	char *mesage =  (char*)"1D10815151515";

	char buffer[1024] = {0};
    char* ip = (char*)"192.168.1.34";

	sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(SER_PORT);
	inet_pton(AF_INET, ip, &serv_addr.sin_addr);

	
    
	
	
	int client_fd;
	struct sockaddr_in address, client_addr;
	int addrlen = sizeof(address);
	
	udpSocket(client_fd, address, PORT);
	bind(client_fd, (struct sockaddr *)&address,sizeof(address));
	//threadedReceive(client_fd);
	std::thread (threadedReceive, client_fd).detach();

	string inquery;
	while(1){
		cout<<">>";
		cin>>inquery;
		printf("Sending to %s, on port %i\n", ip, SER_PORT);
		int iResult = sendto(sock, (const char *)inquery.c_str(), strlen(inquery.c_str()),
			0, (const struct sockaddr *) &serv_addr, 
				1000);
		if(iResult == -1){
			printf("sendto failed with error: %s\n", strerror(errno));
		}
		printf("Message sent\n");
	}

	return 0;
}