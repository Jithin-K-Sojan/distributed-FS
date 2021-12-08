#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <sys/msg.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define MAX_CHUNKSIZE 2000
#define path1 "./d_server.c"
#define path2 "./m_server.c"
#define pathform "./client.c"
#define N 3

struct ctod{
	long mtype;
	int CID;
	char cmd[100];
	char chunk[MAX_CHUNKSIZE];
	int size;
};

struct dtoc{
	long mtype;
	int status;
	int CID;
	char result[2*MAX_CHUNKSIZE];
};

struct dtom{
	long mtype;
	int d_pids[N];
};

struct mtod
{
	long mtype;
	int chunks1[100];
	int size1;
	int chunks2[100];
	int size2;
};

int main()
{
	//creating the directories to be used by data servers
	struct stat st = {0};

	for(int i=1;i<=N;i++)
	{
		char dirname[50];
		char number = i+'0';
		strcpy(dirname,"./d");
		strncat(dirname,&number,1);
		if (stat(dirname, &st) == -1) {
    		mkdir(dirname, 0777);
		}
	}
	
	key_t key6;
	int qid6;
	//open MQ from D to M
	if((key6=ftok(pathform,6))==-1)
	{
		perror("ftok");
		exit(1);
	}
	if((qid6=msgget(key6,IPC_CREAT|0666))==-1)
	{
		perror("msgget");
		exit(1);
	}

	int pid[N];
	pid[0]=getpid();
	int qid4[N],qid5[N],qid3[N];

		//open message Q from M to D server 1
		if((qid3[0]=msgget(ftok(pathform,getpid()+10),IPC_CREAT|0666))==-1)
		{
			perror("msgget");
			exit(1);
		}
	
		//open message Q from client to D server 1
		if((qid4[0]=msgget(ftok(path1,getpid()),IPC_CREAT|0666))==-1)
		{
			perror("msgget");
			exit(1);
		}
		
		//open message Q from D server 1 to client
		if((qid5[0]=msgget(ftok(path2,getpid()),IPC_CREAT|0666))==-1)
		{
			perror("msgget");
			exit(1);
		}

	//need to create N data servers
	for(int i=0;i<N-1;i++)
	{
		if(fork())
			break;
		else
		{
			pid[i+1]=getpid();

			//open message Q from M to D server i+1
			if((qid3[i+1]=msgget(ftok(pathform,getpid()+10),IPC_CREAT|0666))==-1)
			{
				perror("msgget");
				exit(1);
			}

			//open message Q from client to D server i+1
			if((qid4[i+1]=msgget(ftok(path1,getpid()),IPC_CREAT|0666))==-1)
			{
				perror("msgget");
				exit(1);
			}
			//open message Q from D server i+1 to client
			if((qid5[i+1]=msgget(ftok(path2,getpid()),IPC_CREAT|0666))==-1)
			{
				perror("msgget");
				exit(1);
			}

			if(i==N-2){

				struct dtom sendAddr;
				sendAddr.mtype=1;
				for(int j=0;j<N;j++)
				{
					sendAddr.d_pids[j] = pid[j];
				}
				if(msgsnd(qid6,&sendAddr,sizeof(sendAddr)-sizeof(long),0)==-1)
					{
						perror("msgsnd");
						exit(1);
					}
			}
		}
	}

	//pid[0] -> pid of data server 1
	//pid[1] -> pid of data server 2
	//pid[2] -> pid of data server 3
	
	while(1)
	{
		for(int i=0;i<N;i++)
		{
			switch(pid[i]==getpid())
			{
				case 0: break;
				case 1:
				;
				int flag=0;
				struct ctod clientM;
				if(msgrcv(qid4[i],&clientM,sizeof(clientM)-sizeof(long),0,IPC_NOWAIT)==-1) //doesn't wait
				{
					if(errno==ENOMSG) flag=1;
					else
					{	
						perror("msgrcv");
						exit(1);
					}
				}
				if(!flag)
				{
					switch(clientM.mtype)
					{
						case 1: 
						; //write chunk
						char buff1[100];
						char number = i+1+'0';
						strcpy(buff1,"./d");
						strncat(buff1,&number,1);
						strcat(buff1,"/");
						int length = snprintf( NULL, 0, "%d", clientM.CID);
						char* string_CID = malloc( length + 1 );
						snprintf(string_CID, length + 1, "%d", clientM.CID);
						strcat(buff1,string_CID);
						int fd1 = open(buff1,O_CREAT|O_RDWR,0777);
						if(fd1==-1)
						{
							perror("fd");
							exit(1);
						}
						free(string_CID);
						if(write(fd1,clientM.chunk,clientM.size)==-1)
						{
							perror("write");
							exit(1);
						}
						struct dtoc sendStat;
						sendStat.mtype=1;
						sendStat.status=0; //success
						sendStat.CID=clientM.CID;
						if(msgsnd(qid5[i],&sendStat,sizeof(sendStat)-sizeof(long),0)==-1)
						{
							perror("msgsnd");
							exit(1);
						}
						break;
						case 2:
						; //cmd on D server
						struct dtoc sendRes;
						sendRes.mtype=2;
						char dir[50];
						char num = i+1+'0';
						strcpy(dir,"./d");
						strncat(dir,&num,1);
						int fd = open(".", O_RDONLY);
						chdir(dir);
						FILE* fp = popen(clientM.cmd,"r");
						fchdir(fd);
						if(fp==NULL)
						{
							sendRes.status=1;
							if(msgsnd(qid5[i],&sendRes,sizeof(sendRes)-sizeof(long),0)==-1)
							{
								perror("msgsnd");
								exit(1);
							}
						}
						int code=fread(sendRes.result,sizeof(sendRes.result),1,fp);
						if(code==-1||pclose(fp)!=0)
						{
							sendRes.status=1;
							if(msgsnd(qid5[i],&sendRes,sizeof(sendRes)-sizeof(long),0)==-1)
							{
								perror("msgsnd");
								exit(1);
							}	
						}
						else
						{
							sendRes.status=0;
							if(msgsnd(qid5[i],&sendRes,sizeof(sendRes)-sizeof(long),0)==-1)
							{
								perror("msgsnd");
								exit(1);
							}
						}
						break;
					}
				}
				
				else
				{
					struct mtod rmcp;
					if(msgrcv(qid3[i],&rmcp,sizeof(rmcp)-sizeof(long),0,IPC_NOWAIT)==-1)
					{
						if(errno==ENOMSG) break;
						perror("msgrcv");
						exit(1);
					}

					switch(rmcp.mtype)
					{
						case 1:
						;//copy
						char dir[50];
						char num = i+1+'0';
						strcpy(dir,"./d");
						strncat(dir,&num,1);
						int fd = open(".", O_RDONLY);
						chdir(dir);
						for(int k=0;k<rmcp.size2;k++)
						{
							char cmd[100];
							strcpy(cmd,"cp ");
							int length1 = snprintf( NULL, 0, "%d", rmcp.chunks1[k]);
							char* string_CID1 = malloc( length1 + 1 );
							snprintf(string_CID1, length1 + 1, "%d", rmcp.chunks1[k]);
							strcat(cmd,string_CID1);
							strcat(cmd," ");
							int length2 = snprintf( NULL, 0, "%d", rmcp.chunks2[k]);
							char* string_CID2 = malloc( length2 + 1 );
							snprintf(string_CID2, length2 + 1, "%d", rmcp.chunks2[k]);
							strcat(cmd,string_CID2);
							printf("Exec cmd on D%d: %s\n",i+1,cmd);
							FILE* fp = popen(cmd,"r");
							if(fp!=NULL&&fclose(fp)!=0)
							{
								printf("Error in copying chunk %d on D%d\n",rmcp.chunks1[k],i+1);
							}
						}						
						fchdir(fd);
						break;
						case 2: //delete chunks
						;
						int flag;
						for(int k=0;k<rmcp.size1;k++)
						{	
							flag=0;
							char buff1[50];
							char number = i+1+'0';
							strcpy(buff1,"./d");
							strncat(buff1,&number,1);
							strcat(buff1,"/");
							int length = snprintf( NULL, 0, "%d", rmcp.chunks1[k]);
							char* string_CID = malloc( length + 1 );
							snprintf(string_CID, length + 1, "%d", rmcp.chunks1[k]);
							strcat(buff1,string_CID);
							if(remove(buff1)==-1){
								perror("remove");
								flag=1;
								break;
							}
						}
						if(!flag)
						printf("Deleted chunks on server D%d\n",i+1);
					}
				}				
				break;
			}
		}
	}
}