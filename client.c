#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <sys/msg.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#define path "./client.c" //same path used for Qs 1,2,3 and 6
#define MAX_CHUNKSIZE 2000

int qid41=-1,qid42=-1,qid43=-1,qid51=-1,qid52=-1,qid53=-1; //C knows 3 D servers at a time

struct ctom{
	long mtype;
	char forc[100]; //file path or command
};

struct mtoc{
	long mtype;
	char p[100],q[100];
	int d1,d2,d3;
	int CID;
	int status;
	int newCIDs[100];
	int size;
};

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

void printMenu()
{
	printf("Enter operation you wish to perform : \n");
	printf(" 1. Add file\n");
	printf(" 2. Copy file (CP)\n");
	printf(" 3. Move file (MV)\n");
	printf(" 4. Remove file (RM)\n");
	printf(" 5. Send command to D server\n");
	printf(" 6. Exit\n");
}

void addFile(char file[],char filepath[],int chunkSize,int qid1,int qid2)
{
	char buff[chunkSize+1];
	int fd1 = open(file,O_RDONLY);
	if(fd1==-1)
	{
		perror("open");
		return;
	}
	int numRead;
	//request to add file at path filepath
	struct ctom addReq;
	addReq.mtype=1;
	strcpy(addReq.forc,filepath);
	if(msgsnd(qid1,&addReq,sizeof(addReq)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}
	//receiving status back from M
	struct mtoc recvStat;
	if(msgrcv(qid2,&recvStat,sizeof(recvStat)-sizeof(long),1,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}
	else
	{
		if(recvStat.status==0)
			printf("File hierarchy modified!\n");
		else if(recvStat.status==1)
		{
			printf("Given file path not valid!\n");
			return;
		}
		else if(recvStat.status==2)
		{
			printf("File with given path already exists!\n");
			return;
		}
	}

	//send addchunk req
	struct ctom addChunk;
	addChunk.mtype=2;
	strcpy(addChunk.forc,filepath);
	struct mtoc recvAddr;
	while((numRead=read(fd1,buff,chunkSize)))
	{
		if(numRead==-1)
		{
			perror("read");
			return;
		}
		buff[numRead]='\0';

		if(msgsnd(qid1,&addChunk,sizeof(addChunk)-sizeof(long),0)==-1)
		{
			perror("msgsnd");
			exit(1);
		}
		if(msgrcv(qid2,&recvAddr,sizeof(recvAddr)-sizeof(long),2,0)==-1)
		{
			perror("msgrcv");
			exit(1);
		}

		//open message Qs from client to D servers
		if((qid41=msgget(ftok(recvAddr.p,recvAddr.d1),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
		if((qid42=msgget(ftok(recvAddr.p,recvAddr.d2),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
		if((qid43=msgget(ftok(recvAddr.p,recvAddr.d3),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
		//open message Qs from D servers to client
		if((qid51=msgget(ftok(recvAddr.q,recvAddr.d1),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
		if((qid52=msgget(ftok(recvAddr.q,recvAddr.d2),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
		if((qid53=msgget(ftok(recvAddr.q,recvAddr.d3),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
		//send chunk and its ID to D servers
		struct ctod sendChunk;
		sendChunk.mtype=1;
		sendChunk.CID=recvAddr.CID;
		strcpy(sendChunk.chunk,buff);
		sendChunk.size=numRead+1;
		struct dtoc recvStatD1,recvStatD2,recvStatD3;
		//D1
		if(msgsnd(qid41,&sendChunk,sizeof(sendChunk)-sizeof(long),0)==-1)
		{
			perror("msgsnd");
			exit(1);
		}
		if(msgrcv(qid51,&recvStatD1,sizeof(recvStatD1)-sizeof(long),1,0)==-1)
		{
			perror("msgrcv");
			exit(1);
		}
		if(recvStatD1.status)
		{
			printf("Error in storing chunk (ID : %d) at server D1!\n",recvStatD1.CID);
		}
		else
		{
			printf("Chunk (ID : %d) stored at D1!\n",recvStatD1.CID);
		}

		//D2
		if(msgsnd(qid42,&sendChunk,sizeof(sendChunk)-sizeof(long),0)==-1)
		{
			perror("msgsnd");
			exit(1);
		}
		if(msgrcv(qid52,&recvStatD2,sizeof(recvStatD2)-sizeof(long),1,0)==-1)
		{
			perror("msgrcv");
			exit(1);
		}
		if(recvStatD2.status)
		{
			printf("Error in storing chunk (ID : %d) at server D2!\n",recvStatD2.CID);
		}
		else
		{
			printf("Chunk (ID : %d) stored at D2!\n",recvStatD2.CID);
		}
		
		//D3		
		if(msgsnd(qid43,&sendChunk,sizeof(sendChunk)-sizeof(long),0)==-1)
		{
			perror("msgsnd");
			exit(1);
		}
		if(msgrcv(qid53,&recvStatD3,sizeof(recvStatD3)-sizeof(long),1,0)==-1)
		{
			perror("msgrcv");
			exit(1);
		}
		if(recvStatD3.status)
		{
			printf("Error in storing chunk (ID : %d) at server D3!\n",recvStatD3.CID);
		}
		else
		{
			printf("Chunk (ID : %d) stored at D3!\n",recvStatD3.CID);
		}

	}
	close(fd1);
}

void cpFile(char cmd[],int qid1,int qid2)
{
	struct ctom copy;
	copy.mtype=3;
	strcpy(copy.forc,cmd);
	//send copy request to M
	if(msgsnd(qid1,&copy,sizeof(copy)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}

	//receive status 
	struct mtoc copyStat;
	if(msgrcv(qid2,&copyStat,sizeof(copyStat)-sizeof(long),3,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}

	if(copyStat.status==0)
	{
		printf("File hierarchy modified!\n");
		printf("IDs of chunks added : ");
		for(int i=0;i<copyStat.size;i++)
		{
			printf("%d ",copyStat.newCIDs[i]);
		}
		printf("\n");
	}
	else if(copyStat.status==1)
	{
		printf("Source file with given path does not exist!\n");
	}
	else if(copyStat.status==2)
	{
		printf("Source or destination file path is not valid!\n");
	}
	else if(copyStat.status==3)
	{
		printf("Check command again!\n");
	}

}

void mvFile(char cmd[],int qid1,int qid2)
{
	struct ctom move;
	move.mtype=4;
	strcpy(move.forc,cmd);
	//send move request to M
	if(msgsnd(qid1,&move,sizeof(move)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}

	//receive status
	struct mtoc moveStat;
	if(msgrcv(qid2,&moveStat,sizeof(moveStat)-sizeof(long),4,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}

	if(moveStat.status==0)
	{
		printf("File hierarchy modified!\n");
	}
	else if(moveStat.status==1)
	{
		printf("Source file with given path does not exist!\n");
	}
	else if(moveStat.status==2)
	{
		printf("Source or destination file path is not valid!\n");
	}
	else if(moveStat.status==3)
	{
		printf("Check command again!\n");
	}
}

void rmFile(char rmcmd[],int qid1,int qid2)
{
	struct ctom rm;
	rm.mtype=5;
	strcpy(rm.forc,rmcmd);
	//send remove request to M
	if(msgsnd(qid1,&rm,sizeof(rm)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}

	//receive sttaus
	struct mtoc rmStat;
	if(msgrcv(qid2,&rmStat,sizeof(rmStat)-sizeof(long),5,0)==-1)
	{
		perror("msgrcv1");
		exit(1);
	} 

	if(rmStat.status==0)
	{
		printf("Succesfully deleted!\n");
	}
	else if(rmStat.status==1)
	{
		printf("Source file with given path does not exist!\n");
	}
	else if(rmStat.status==2)
	{
		printf("Source or destination file path is not valid!\n");
	}
	else if(rmStat.status==3)
	{
		printf("Check command again!\n");
	}
}

void sendCmd(char cmdD[])
{
	struct ctod cmd2d;
	cmd2d.mtype=2;
	strcpy(cmd2d.cmd,cmdD);
	struct dtoc res1,res2,res3;
	//send cmd to 3 D servers and receive statuses
	//D1
	if(msgsnd(qid41,&cmd2d,sizeof(cmd2d)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}
	if(msgrcv(qid51,&res1,sizeof(res1)-sizeof(long),2,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}
	if(res1.status==0)
		printf("Result from D1 : \n%s\n",res1.result);
	else printf("Error (D1)!\n");

	//D2
	if(msgsnd(qid42,&cmd2d,sizeof(cmd2d)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}
	if(msgrcv(qid52,&res2,sizeof(res2)-sizeof(long),2,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}
	if(res1.status==0)
		printf("Result from D2 : \n%s\n",res2.result);
	else printf("Error (D2)!\n");

	//D3
	if(msgsnd(qid43,&cmd2d,sizeof(cmd2d)-sizeof(long),0)==-1)
	{
		perror("msgsnd");
		exit(1);
	}
	if(msgrcv(qid53,&res3,sizeof(res3)-sizeof(long),2,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}
	if(res1.status==0)
		printf("Result from D3 : \n%s\n",res3.result);
	else printf("Error (D3)!\n");
}

int main()
{
	char file[100],filepath[100],cpcmd[100],mvcmd[100],rmcmd[100],cmdD[100];
	int chunkSize;
	//Create or open message queues 1 (C to M), 2 (M to C)
	key_t key1,key2;
	int qid1,qid2;
	if((key1=ftok(path,1))==-1)
	{
		perror("ftok");
		exit(1);
	}
	if((key2=ftok(path,2))==-1)
	{
		perror("ftok");
		exit(1);
	}
	if((qid1=msgget(ftok(path,1),IPC_CREAT|0666))==-1)
	{
		perror("msgget");
		exit(1);
	}
	if((qid2=msgget(key2,IPC_CREAT|0666))==-1)
	{
		perror("msgget");
		exit(1);
	}

	while(1)
	{
		int choice;
		printMenu();
		scanf("%d",&choice);
		getchar();
		switch(choice)
		{
			case 1: 
			printf("Enter path (on system) of file to be added (like: /home/samina/f1.txt) : ");
			fgets(file,99,stdin);
			file[strcspn(file,"\n")] = '\0';
			printf("Enter path you want this file to be stored at (like: /home/duser/newname.txt) : ");
			fgets(filepath,99,stdin);
			filepath[strcspn(filepath,"\n")] = '\0';
			do
			{
				printf("Enter chunk size (less than %d) : ",MAX_CHUNKSIZE);
				scanf("%d",&chunkSize);
				getchar();
			}while(chunkSize>MAX_CHUNKSIZE);		
			addFile(file,filepath,chunkSize,qid1,qid2);
			break;
			case 2:
			printf("Enter cp cmd with src and destination: ");
			fgets(cpcmd,99,stdin);
			cpcmd[strcspn(cpcmd,"\n")] = '\0';
			cpFile(cpcmd,qid1,qid2);
			break;
			case 3:
			printf("Enter mv cmd with src and destination: ");
			fgets(mvcmd,99,stdin);
			mvcmd[strcspn(mvcmd,"\n")] = '\0';
			mvFile(mvcmd,qid1,qid2);
			break;
			case 4:
			printf("Enter rm cmd with full path of file to be removed: ");
			fgets(rmcmd,99,stdin);
			rmcmd[strcspn(rmcmd,"\n")] = '\0';
			rmFile(rmcmd,qid1,qid2);
			break;
			case 5:
			if(qid41==-1||qid42==-1||qid43==-1||qid51==-1||qid52==-1||qid53==-1)
			{
				printf("No file has been added!\n");
				break;
			}
			printf("Enter command with file name as CHUNK ID: ");
			fgets(cmdD,99,stdin);
			cmdD[strcspn(cmdD,"\n")] = '\0';
			sendCmd(cmdD);
			break;
			case 6:
			exit(0);
			break;
			default: printf("Invalid choice entered!\n");
		}

	}
	
}
