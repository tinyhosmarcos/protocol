// g++ slave.cpp -lpthread -lsqlite3 -o oslave.out
// oslave.out <ipMaster><portMaster><ip propia><puerto propio>
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
#include <sqlite3.h>

using namespace std;

#define PORT 1234

struct slaves{
	int len = 0;
	map<int, string> ips;
	map<int, int> ports;
};

struct slaves fellow_slave_list;
map<string,string> messages_buffer;
vector<string> retornoDB;

struct atributo {
	string nodo_nombre;
	string atributo_nombre;
	string valor;
};
struct relacion{
	string nodo_origen;
	string nodo_destino;
};


//funcrep

int calc_slaves_id(string buffer){
	//is_complete and action 2 bytes
	//namesize 3 bytes
	int namesize = stoi(buffer.substr(2,3));
	string node_name = buffer.substr(10,namesize);
	int idx = (int)(node_name[0]) % fellow_slave_list.len;
	return idx;
}
void udpSocket(int &sock, struct sockaddr_in &serv_addr, int port, string ip="")
{
    sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);
	if(ip == "")
		serv_addr.sin_addr.s_addr = INADDR_ANY;
	else
		inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);
}
void threaded_send(string message, string ip, int port){
		int sockId;
		struct sockaddr_in address;
		int iResult;
		
		udpSocket(sockId, address, port, ip);
		iResult = sendto(sockId, (const char *)message.c_str(), strlen(message.c_str()),
			0, (const struct sockaddr *) &address, 
				sizeof(address));
		if(iResult == -1){
	        printf("sendto failed with error: %s\n", strerror(errno));
		}
		close(sockId);
}

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
   int i;
   printf("|");
   for(i = 0; i<argc; i++) {
      //printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
      printf("%s|", argv[i] ? argv[i] : "NULL");
	  if(argv[i]){
		  //retornoDB+=+argv[i];
		  //retornoDB+=" ";
		  retornoDB.push_back(argv[i]);
	  }
	  else{
		  retornoDB.push_back("NULL");
	  }
	  //argv[i] ? retornoDB+=+argv[i] : retornoDB+="NULL";
	  
	  
   }
   printf("\n");
   return 0;
}
void print_database(string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    char const *query;
    sqlite3_open(dbName.c_str(), &db);
	printf("Nodos:\n");
	printf("|nodo_nombre|:\n");
	sqlite3_exec(db, (char*)"select * from Nodos", callback, 0, &zErrMsg);
	printf("Atributos:\n");
	printf("|nodo_nombre|atributo_nombre|valor|:\n");
	sqlite3_exec(db, (char*)"select * from Atributos", callback, 0, &zErrMsg);
	printf("Relaciones:\n");
	printf("|nodo_origen|dodo_destino|:\n");
	sqlite3_exec(db, (char*)"select * from Relaciones", callback, 0, &zErrMsg);
	sqlite3_close(db);
}
bool nodeExists(string nodo, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    //char const *query;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "select * from nodos ";
	query += "where nodo_nombre = ";
	query += "\"" + nodo + "\"";
	query += ";";
	printf("%s\n",query.c_str());
	retornoDB.clear();
	sqlite3_exec(db, query.c_str(), callback, 0, &zErrMsg);
	sqlite3_close(db);
	
	if(retornoDB.empty()) return false;
	else return true;
}
void getRelations(string nodo, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    //char const *query;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "select nodo_destino from relaciones ";
	query += "where nodo_origen = ";
	query += "\"" + nodo + "\"";
	query += ";";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), callback, 0, &zErrMsg);
	sqlite3_close(db);
}
void getAttributes(string nodo, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    //char const *query;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "select atributo_nombre, valor from atributos ";
	query += "where nodo_nombre = ";
	query += "\"" + nodo + "\"";
	query += ";";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), callback, 0, &zErrMsg);
	sqlite3_close(db);
}
//delete nodos, atributos y relaciones "nodo" en "dbName"
void delete_nodo(string nodo, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "delete from nodos ";
	query += "where ";
	query += "nodo_nombre =\"" +nodo+ "\"";
	query += ";";
	string query2;
	query2 += "delete from atributos ";
	query2 += "where ";
	query2 += "nodo_nombre =\"" +nodo+ "\"";
	query2 += ";";
	string query3;
	query3 += "delete from relaciones ";
	query3 += "where ";
	query3 += "nodo_origen =\"" +nodo+ "\"";
	query3 += ";";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), 0, 0, &zErrMsg);
	sqlite3_exec(db, query2.c_str(), 0, 0, &zErrMsg);
	sqlite3_exec(db, query3.c_str(), 0, 0, &zErrMsg);
	sqlite3_close(db);
}
void delete_atributo(string nodo,string atributo, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "delete from atributos ";
	query += "where ";
	query += "nodo_nombre =\"" +nodo+ "\" and atributo_nombre = \""+atributo+ "\"";
	query += ";";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), 0, 0, &zErrMsg);
	sqlite3_close(db);
}
void delete_relacion(string nodo, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "delete from relaciones ";
	query += "where ";
	query += "nodo_destino =\"" +nodo+ "\"";
	query += ";";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), 0, 0, &zErrMsg);
	sqlite3_close(db);
}

void insert_relacion(relacion rel, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    //char const *query;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "INSERT INTO Relaciones ";
	query += "VALUES (";
	query += "\"" +rel.nodo_origen+ "\",";
	query += "\"" +rel.nodo_destino+ "\"";
	query += ");";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), 0, 0, &zErrMsg);
	sqlite3_close(db);
}
void insert_nodo(string nodo_name, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "INSERT INTO Nodos ";
	query += "VALUES (";
	query += "\"" +nodo_name+ "\"";
	query += ");";
	printf("%s\n",query.c_str());
	if(!nodeExists(nodo_name, dbName)){
		sqlite3_exec(db, query.c_str(), 0, 0, &zErrMsg);
	}
	sqlite3_close(db);
}
void insert_atributo(atributo attr, string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    sqlite3_open(dbName.c_str(), &db);
	string query;
	query += "INSERT INTO Atributos ";
	query +="VALUES (";
	query +="\"" + attr.nodo_nombre+"\",";
    query +="\"" +attr.atributo_nombre+"\",";
	query +="\"" +attr.valor+"\"";
    query +=");";
	printf("%s\n",query.c_str());
	sqlite3_exec(db, query.c_str(), 0, 0, &zErrMsg);
	sqlite3_close(db);
}


void create_database_if_not_exists(string dbName){
	sqlite3 *db;
    char *zErrMsg = 0;
    char *sqlAttr, *sqlRela, *sqlNodo;
    sqlite3_open(dbName.c_str(), &db);
	sqlNodo = (char*)("CREATE TABLE IF NOT EXISTS Nodos("  \
      "nodo_nombre varchar(255)" \
      ");");
    sqlAttr = (char*)("CREATE TABLE IF NOT EXISTS Atributos("  \
      "nodo_nombre varchar(255)," \
      "atributo_nombre varchar(255)," \
      "valor varchar(100000)" \
      ");");
    sqlRela = (char*)("CREATE TABLE IF NOT EXISTS Relaciones("  \
      "nodo_origen varchar(255)," \
      "nodo_destino varchar(255)" \
      ");");
	sqlite3_exec(db, sqlNodo, 0, 0, &zErrMsg);
    sqlite3_exec(db, sqlAttr, 0, 0, &zErrMsg);
    sqlite3_exec(db, sqlRela, 0, 0, &zErrMsg);

    sqlite3_close(db);
}
//1D205Julio15Grado academico
string buildDeleteRelQuery(string node_name, string node_to_delete){
	string query = "1D3";
	char tmp[20];
	sprintf(tmp, "%02d", (int)node_name.size());
	query += string(tmp);
	query += node_name;
	sprintf(tmp, "%02d", (int)node_to_delete.size());
	query += string(tmp);
	query += node_to_delete;
	return query;
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

void message_parser(string message, string dbName, string source_ip){
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
		int id = stoi(message.substr(2,2));
		string ip = message.substr(4,15);
		int port = stoi(message.substr(19,4));
		fellow_slave_list.ips.insert(pair<int, string>(id, ip));
		fellow_slave_list.ports.insert(pair<int, int>(id, port));
		printf("new slave %i, %s on port %i\n", id, ip.c_str(), port);
		fellow_slave_list.len = fellow_slave_list.ips.size();
	}
	else if (action == 'C'){
		int namesize = stoi(message.substr(2,3));
		int numOfRels = stoi(message.substr(5,3));
		int numOfAttribs = stoi(message.substr(8,2));
		string nodeName = message.substr(10,namesize);
		int buffPosRead= 10 + namesize;

		insert_nodo(nodeName, dbName);
		printf("Nodo procesado\n");
		//leer relaciones
		for( int i = 0; i < numOfRels; i++){
			relacion rel;
			rel.nodo_origen = nodeName;
			int relNodeNameSize = stoi(message.substr(buffPosRead, 3));
			buffPosRead += 3;
			rel.nodo_destino = message.substr(buffPosRead, relNodeNameSize);
			buffPosRead += relNodeNameSize;
			printf("relacion procesada\n");
			insert_relacion(rel, dbName);

			vector<string> attN, attV, relations;
			relations.push_back(rel.nodo_origen);
			string queryMessage = buildCreateQuery(rel.nodo_destino, attN, attV, relations );
			queryMessage[1] = 'c';
			int slave_id;
			slave_id = calc_slaves_id(queryMessage);
			int slave_port = fellow_slave_list.ports.at(slave_id);
			string  slave_ip = fellow_slave_list.ips.at(slave_id);
			printf("Spread to slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
			threaded_send(queryMessage, slave_ip, slave_port);
		}
		for( int i = 0; i < numOfAttribs; i++){
			atributo attr;
			attr.nodo_nombre = nodeName;
			int attribNameSize = stoi(message.substr(buffPosRead, 3));
			buffPosRead += 3;
			int attribValueSize = stoi(message.substr(buffPosRead, 3));
			buffPosRead += 3;
			attr.atributo_nombre = message.substr(buffPosRead, attribNameSize);
			buffPosRead += attribNameSize;
			attr.valor = message.substr(buffPosRead, attribValueSize);
			buffPosRead += attribValueSize;
			printf("aributo procesado\n");
			insert_atributo(attr, dbName);
		}
		print_database(dbName);
	}
	else if (action == 'c'){
		int namesize = stoi(message.substr(2,3));
		int numOfRels = stoi(message.substr(5,3));
		int numOfAttribs = stoi(message.substr(8,2));
		string nodeName = message.substr(10,namesize);
		int buffPosRead= 10 + namesize;

		insert_nodo(nodeName, dbName);
		printf("Nodo procesado\n");
		//leer relaciones
		for( int i = 0; i < numOfRels; i++){
			relacion rel;
			rel.nodo_origen = nodeName;
			int relNodeNameSize = stoi(message.substr(buffPosRead, 3));
			buffPosRead += 3;
			rel.nodo_destino = message.substr(buffPosRead, relNodeNameSize);
			buffPosRead += relNodeNameSize;
			printf("relacion procesada\n");
			insert_relacion(rel, dbName);
		}
		for( int i = 0; i < numOfAttribs; i++){
			atributo attr;
			attr.nodo_nombre = nodeName;
			int attribNameSize = stoi(message.substr(buffPosRead, 3));
			buffPosRead += 3;
			int attribValueSize = stoi(message.substr(buffPosRead, 3));
			buffPosRead += 3;
			attr.atributo_nombre = message.substr(buffPosRead, attribNameSize);
			buffPosRead += attribNameSize;
			attr.valor = message.substr(buffPosRead, attribValueSize);
			buffPosRead += attribValueSize;
			printf("aributo procesado\n");
			insert_atributo(attr, dbName);
		}
		print_database(dbName);
	}
	else if(action == 'R'){
		//1R081015151515
		int namesize = stoi(message.substr(2,2));
		int deep = stoi(message.substr(4,1));
		int attributes = stoi(message.substr(5,1));
		string node_name = message.substr(6,namesize);
		//int idx = (int)(node_name[0]) % slave_list.len;
		retornoDB.clear();
		string mensaje_retorno="";
		if(deep == 1){
			getRelations(node_name, dbName);
			for(int i=0;i<retornoDB.size();i++){
				mensaje_retorno+=retornoDB[i]+" ";
			}
		}
		if(attributes == 1){
			getAttributes(node_name, dbName);
			for(int i=0;i<retornoDB.size();i++){
				mensaje_retorno+=retornoDB[i]+" ";
			}
		}
		mensaje_retorno="1r"+mensaje_retorno;
		threaded_send(mensaje_retorno, source_ip, 1200);
	}
	else if(action == 'D'){
		//1D205Julio15Grado academico
		int opcion = stoi(message.substr(2,1));
		int namesize = stoi(message.substr(3,2));
		string node_name = message.substr(5,namesize);
		if(opcion ==1){
			retornoDB.clear();
			getRelations(node_name, dbName);
			for(int i=0;i<retornoDB.size();i++){
				
				string queryMessage = buildDeleteRelQuery(retornoDB[i], node_name);
				
				//send delete
				int slave_id;
				slave_id = (int)(retornoDB[i][0]) % fellow_slave_list.len;
				int slave_port = fellow_slave_list.ports.at(slave_id);
				string  slave_ip = fellow_slave_list.ips.at(slave_id);
				printf("Spread to slave %d, %s on port %d\n", slave_id, slave_ip.c_str(), slave_port);
				threaded_send(queryMessage, slave_ip, slave_port);
			}
			delete_nodo(node_name, dbName);
		}
		if(opcion==2){
			int attribNameSize = stoi(message.substr(5+namesize,2));
			string attrib_name = message.substr(7+namesize, namesize);
			delete_atributo(node_name, attrib_name, dbName);
		}
		if(opcion==3){
			int nodoDeleteNameSize = stoi(message.substr(5+namesize,2));
			string nodoDelete_name = message.substr(7+namesize, namesize);
			delete_relacion(nodoDelete_name, dbName);
		}
		print_database(dbName);
	}
	
}
void subscribeToMaster(string master_ip, int master_port, string self_ip, string self_port){

	string message = "1S"+self_ip+self_port;

	int serverSockId;
	struct sockaddr_in server_address;
	udpSocket(serverSockId, server_address, master_port, master_ip);
	sendto(serverSockId, (const char *)message.c_str(), strlen(message.c_str()),
				0, (const struct sockaddr *) &server_address, 
					sizeof(server_address));
	close(serverSockId);
}
int main(int argc, char const *argv[])
{
	subscribeToMaster(argv[1], atoi(argv[2]), argv[3], argv[4]);

	int server_fd;
	struct sockaddr_in address, client_addr;
	int addrlen = sizeof(address);
	char buffer[1024] = {0};
	
	udpSocket(server_fd, address, atoi(argv[4]));
	bind(server_fd, (struct sockaddr *)&address,sizeof(address));
	
	string dbName = "dbShard"+string(argv[4])+".db";
	create_database_if_not_exists(dbName);

	printf("Slave waiting message in port %s\n", argv[4]);
	memset(buffer,0,1000);
	while(1){
		int iResult = recvfrom(server_fd, (char *)buffer, 1000, 
			0, ( struct sockaddr *) &client_addr, 
			(socklen_t*)&addrlen);
		if(iResult == -1){
			printf("recvfrom failed with error %s\n", strerror(errno));
		}
		char *clientIp = inet_ntoa(client_addr.sin_addr);
		string sbuffer(buffer);
		printf("Node %s says: %s\n",clientIp,buffer);
		message_parser(sbuffer, dbName, inet_ntoa(client_addr.sin_addr));
	}
	
	
	return 0;
}
