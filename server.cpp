/*
prg.exe  <C> <node>  [<attribute-name> <attribute-value>...] [<relationship-node-name>]
prg.exe  <R> <node>  <node> <level 1..99> <include-attributes 1/0>
prg.exe  <U> <node>  <node N | attribute A> < <name-node><new-name> | <name|value><attribute-name><new-attribute-name | new-attribute-name> >
prg.exe  <D> <node>  <node N | attribute A> < <name-node> | <attribute-name> >
prg.exe  <Q> .... opcional + seria un plus
*/
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <thread>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <cstdlib>

using namespace std;

#define PORT 1200

struct slaves{
	int len = 0;
	map<int, string> ips;
	map<int, int> ports;
};
struct slaves slave_list;

string ip_retorno;
map<string,string> messages_buffer;

//instancia un socket udp con puerto y ip
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
string buildDeleteQuery(string node_name){
	string query = "1D1";
	char tmp[20];
	sprintf(tmp, "%02d", (int)node_name.size());
	query += string(tmp);
	query += node_name;
	return query;
}
vector<string> scatterMessage(string message){
	int size=(int)(message.size())/(int)(1000);
	if((int)(message.size())%(int)(1000)!=0){
		size+=1;
	}
	vector<string> a;
	return  a;
}

int calc_slaves_id(string buffer){
	//is_complete and action 2 bytes
	//namesize 3 bytes
	char action = (buffer.substr(1,1))[0];
	
	if(action == 'C'){
		int namesize = stoi(buffer.substr(2,3));
		string node_name = buffer.substr(10,namesize);
		int idx = (int)(node_name[0]) % slave_list.len;
		return idx;
	}
	if(action == 'R'){
		//1R081015151515
		int namesize = stoi(buffer.substr(2,2));
		string node_name = buffer.substr(6,namesize);
		int idx = (int)(node_name[0]) % slave_list.len;
		return idx;
	}
	if(action == 'D'){
		//1D105Julio
		int namesize = stoi(buffer.substr(3,2));
		string node_name = buffer.substr(5,namesize);
		int idx = (int)(node_name[0]) % slave_list.len;
		return idx;
	}
	return -1;
}

void threaded_send(string message, string ip, int port){
		int sockId;
		struct sockaddr_in address;
		int iResult;
		
		udpSocket(sockId, address, port, ip);
		
		//scatter

		iResult = sendto(sockId, (const char *)message.c_str(), strlen(message.c_str()),
			0, (const struct sockaddr *) &address, 
				sizeof(address));
		if(iResult == -1){
	        printf("sendto failed with error: %s\n", strerror(errno));
		}
		close(sockId);
}
string buildCreateQuery(string node_name, vector<string> attrib_name, vector<string> attrib_value, vector<string> relations){
	string query = "1C";
	char tmp[20];
	sprintf(tmp, "%03d", (int)node_name.size());
	query += string(tmp);
	sprintf(tmp, "%03d", (int)relations.size());
	query += string(tmp);//num of rels
	sprintf(tmp, "%02d", (int)attrib_name.size());
	query += string(tmp);//num of attribs
	query += node_name;
	for(int i=0;i<relations.size();i++){
		sprintf(tmp, "%03d", (int)relations[i].size());
		query += string(tmp);//node rel size
		query += relations[i];
	}
	for(int i=0;i<attrib_name.size();i++){
		sprintf(tmp, "%03d", (int)attrib_name[i].size());
		query += string(tmp);//name attrib size
		sprintf(tmp, "%03d", (int)attrib_value[i].size());
		query += string(tmp);//value attrib size
		query += attrib_name[i];
		query += attrib_value[i];
	}
	return query;
}
void messageParser(string message, string source_ip){
	char is_complete = (message.substr(0,1))[0];
	//si no es mensaje completo 
	if(is_complete == '0'){
		//si no existe, crea una entrada en map messages_buffer
		if(messages_buffer.find(source_ip)==messages_buffer.end()){
			message = message.substr(1, message.size()-1);
			messages_buffer.insert(pair<string, string>(source_ip, "1" + message));
		}
		//si ya existe, adiciona a la entrada en messages_buffer
		else{
			message = message.substr(1, message.size()-1);
			messages_buffer[source_ip] = messages_buffer[source_ip] + message;
		}
		
		return;
	}
	else{
		//si hay mensajes del mismo origen en buffer
		if(messages_buffer.find(source_ip) != messages_buffer.end()){
			message = message.substr(1, message.size()-1);
			message = messages_buffer[source_ip] + message;
			messages_buffer.erase(source_ip);
		}
	}
	
	char action = (message.substr(1,1))[0];
	
	if(action == 'S'){
		string ip = message.substr(2,15);
		int port = stoi(message.substr(17,4));
		slave_list.ips.insert(pair<int, string>(slave_list.len, ip));
		slave_list.ports.insert(pair<int, int>(slave_list.len, port));
		printf("new slave %i, %s on port %i\n", slave_list.len, ip.c_str(), port);
		slave_list.len = slave_list.ips.size();
		//broadcast informacion de slaves hacia slaves
		for(int i = 0;i < slave_list.len; i++){
			for(int j = 0;j < slave_list.len; j++){
				char cid[20];
				char cport[20];
				sprintf(cid, "%02d",j);
				sprintf(cport, "%04d",slave_list.ports.at(j));
				string new_message = "1S" + string(cid) + slave_list.ips.at(j) + string(cport);
				int slave_port = slave_list.ports.at(i);
				string slave_ip = slave_list.ips.at(i);
				threaded_send(new_message, slave_ip, slave_port);
			}
			//threaded_send(message, slave_ip, slave_port);
		}
	}
	else if (action == 'C'){
		
		int slave_id;
		slave_id = calc_slaves_id(message);
		printf("Send message:\n");

		int slave_port = slave_list.ports.at(slave_id);
		string  slave_ip = slave_list.ips.at(slave_id);
		printf("To slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
		threaded_send(message, slave_ip, slave_port);
		
	}
	else if(action == 'R'){
		int slave_id;
		slave_id = calc_slaves_id(message);
		printf("Send message:\n");

		int slave_port = slave_list.ports.at(slave_id);
		string  slave_ip = slave_list.ips.at(slave_id);
		printf("To slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
		threaded_send(message, slave_ip, slave_port);
		ip_retorno = source_ip;
		//return_list.ips.insert(pair<int, string>(slave_list.len, source_ip));
		//return_list.ports.insert(pair<int, int>(slave_list.len, 1205));//puerto de clientes
	}
	else if(action == 'r'){
		threaded_send(message, ip_retorno, 1205);
	}
	else if(action == 'U'){
		//1U1 05 JULIO 04 OMAR 015 GRADO ACADEMICO 002 DR
		int base=2;
		int opcion = stoi(message.substr(base,1));
		base +=1;
		int namesize = stoi(message.substr(base,2));
		base+=2;
		string node_name = message.substr(base,namesize);
		base+=namesize;
		namesize = stoi(message.substr(base,2));
		base+=2;
		string new_node_name = message.substr(base,namesize);
		base+=namesize;
		
		string deletemessage = buildDeleteQuery(node_name);
		int slave_id;
		slave_id = calc_slaves_id(deletemessage);
		printf("Send message:\n");

		int slave_port = slave_list.ports.at(slave_id);
		string  slave_ip = slave_list.ips.at(slave_id);
		printf("To slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
		threaded_send(deletemessage, slave_ip, slave_port);

		vector<string> attrib_name, attrib_value, relations;
		string createmessage = buildCreateQuery(new_node_name, attrib_name, attrib_value, relations);
		slave_id = calc_slaves_id(createmessage);
		printf("Send message:\n");

		slave_port = slave_list.ports.at(slave_id);
		slave_ip = slave_list.ips.at(slave_id);
		printf("To slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
		threaded_send(createmessage, slave_ip, slave_port);

	}
	else if(action == 'D'){
		/*
		int "flag" is_complete      1B                    1
		int node_or_attribute; // 1:node 2:attibute       1
		int  query_node_size ; //  2B                     05 
		char query_value_node[99];  //  VB                Julio

		int query_attribute_o_R_size;  //          3B        15
		int query_value_attribute_o_R_size; //  3B           Grado academico
		1105Julio
		1205Julio15Grado academico
		*/
		int slave_id;
		slave_id = calc_slaves_id(message);
		printf("Send message:\n");

		int slave_port = slave_list.ports.at(slave_id);
		string  slave_ip = slave_list.ips.at(slave_id);
		printf("To slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
		threaded_send(message, slave_ip, slave_port);
		
	}
}
int main(int argc, char const *argv[])
{
	int server_fd;
	struct sockaddr_in address, client_addr;
	int addrlen = sizeof(address);
	char buffer[1024] = {0};
	
	udpSocket(server_fd, address, PORT);
	bind(server_fd, (struct sockaddr *)&address,sizeof(address));
	
	printf("waiting message in port %i\n", PORT);
	while(1){
		memset(buffer, 0, 1024);
		recvfrom(server_fd, (char *)buffer, 1000, 
			0, ( struct sockaddr *) &client_addr, 
			(socklen_t*)&addrlen);
		
		char *clientIp = inet_ntoa(client_addr.sin_addr);
		string sbuffer(buffer, 1000);
		printf("%s says: %s\n",clientIp,buffer);

		messageParser(sbuffer, inet_ntoa(client_addr.sin_addr));
	}
	
	
	return 0;
}

//master 1200
//cliete 1205
//1210