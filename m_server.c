#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <sys/msg.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

#define path "./client.c" //same path used for Qs 1,2,3 and 6
#define path1 "./d_server.c"
#define path2 "./m_server.c"
#define N 3

int pids[N];

struct TrieNode{
	struct TrieNode *children[64]; //assume file paths are case sensitive, can contain numbers, . and /
	int *CIDs;
	int size; //size of dynamic CIDs array
	int end; //0 for end, nonzero if not end of path
};

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

struct dtom
{
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

struct TrieNode *getNode(void) //create new TrieNode
{ 
    struct TrieNode *pNode = NULL; 
  
    pNode = (struct TrieNode *)malloc(sizeof(struct TrieNode)); 
  
    if (pNode) 
    { 
        int i; 
  
        pNode->CIDs=NULL; //default
  		pNode->end=1; //default
  		pNode->size=-1; //default

        for (i=0;i<64;i++) 
            pNode->children[i] = NULL; 
    } 
  
    return pNode; 
} 

int charToInd(char a)
{
	if(a<='Z'&&a>='A') return a-'A';
	else if(a<='z'&&a>='a') return a-'a'+26;
	else if(a<='9'&&a>='0') return a-'0'+52;
	else if(a=='.') return 62;
	else if(a=='/') return 63;
}

int genCID(int *chunkIDs,int size)
{
	srand(time(0));
	int r;
	int flag=0; //set if r found in current list of CIDs
	do
	{
		r=rand();
		flag=0;
		for(int i=0;i<size;i++)
		{	
			if(chunkIDs[i]==r)
			{
				flag=1;
				break;
			}
		}	
	}while(flag);
	return r;
}

void addCID(struct TrieNode *root,char key[],int CID) //add chunk ID to file path
{
	int i=0,index;
	struct TrieNode *pCrawl = root;

	while(key[i]!='\0')
	{
		index=charToInd(key[i]);
		pCrawl = pCrawl->children[index];
		i++;
	}
	pCrawl->size++;
	pCrawl->CIDs = (int*)realloc(pCrawl->CIDs,sizeof(int)*pCrawl->size);
	pCrawl->CIDs[pCrawl->size-1] = CID;
}

int insert(struct TrieNode *root,char key[]) //insert file path in Trie if valid
{  
    int i=0,index; 
  
    struct TrieNode *pCrawl = root; 
  
    while(key[i]!='\0')
    { 
    	if(pCrawl->end==0&&key[i]=='/') return 1; //file invalid
        index = charToInd(key[i]);
        if (!pCrawl->children[index]) 
            pCrawl->children[index] = getNode();   
        pCrawl = pCrawl->children[index]; 
        i++;
    } 
  
    if(!pCrawl->end) return 2; //file exists 
    if(pCrawl->children[63]) return 1; //file invalid
    pCrawl->end = 0;
    pCrawl->size=0;
    return 0; //file hierarchy succesfully modified
}

int*  delUtil(struct TrieNode* root, char key[],int *size) //to get chunk IDs of file to be deleted
{
	int i=0,index; 
  
    struct TrieNode *pCrawl = root; 
  
    while(key[i]!='\0')
    { 
        index = charToInd(key[i]);  
        pCrawl = pCrawl->children[index]; 
        i++;
    } 
    *size=pCrawl->size;
    return pCrawl->CIDs;  
}

int exist(struct TrieNode *root,char key[]) //checks if file exists in current hierarchy
{
	int i=0,index; 
    struct TrieNode *pCrawl = root; 
  	if(root==NULL) return 0;
    while(key[i]!='\0')
    { 
        index = charToInd(key[i]);
        if (!pCrawl->children[index]) 
            return 0;
        pCrawl = pCrawl->children[index]; 
        i++;
    } 
    if(!pCrawl->end) return 1; //file exists
    return 0;
}


int isEmpty(struct TrieNode* root) //used in Trie delete
{ 
    for (int i = 0; i < 64; i++) 
        if (root->children[i]) 
            return 0; 
    return 1; 
} 
  
struct TrieNode* removeK(struct TrieNode* root, char key[], int len, int depth) // Recursive function to delete a key from given Trie 
{ 
    if (!root) 
        return NULL; 
  
    // If last character of key is being processed 
    if (depth == len) { 
  
        // This node is no more end of word after 
        // removal of given key 
        if (!root->end) 
            root->end = 1; 
  
        // If given is not prefix of any other word 
        if (isEmpty(root)) { 
            free(root); 
            root = NULL; 
        } 
  
        return root; 
    } 
  
    // If not last character, recur for the child 
    // obtained using ASCII value 
    int index = charToInd(key[depth]); 
    root->children[index] =  
          removeK(root->children[index], key, len,depth + 1); 
  
    // If root does not have any child (its only child got  
    // deleted), and it is not end of another word. 
    if (isEmpty(root) && root->end) { 
        free(root); 
        root = NULL; 
    } 
  
    return root; 
}

int* copyFile(struct TrieNode* root,char cpsrc[],char cpdest[],int *chunkIDs,int *assigned,int qid3[N],int qid2) 
{
	int j=0,index;
	struct TrieNode *pCrawl = root;

	while(cpsrc[j]!='\0')
	{
		index=charToInd(cpsrc[j]);
		pCrawl = pCrawl->children[index];
		j++;
	}

	struct mtod cpChunks;
	cpChunks.mtype=1;
	for(int i=0;i<pCrawl->size;i++)
		cpChunks.chunks1[i] = pCrawl->CIDs[i];
	cpChunks.size1=pCrawl->size;
	cpChunks.size2=pCrawl->size;
	for(int i=0;i<pCrawl->size;i++) //generate new CIDs
	{
		(*assigned)++;
		chunkIDs = (int*) realloc(chunkIDs,sizeof(int)*(*assigned));
		cpChunks.chunks2[i] = genCID(chunkIDs,*assigned-1);
		chunkIDs[*assigned-1] = cpChunks.chunks2[i];
		addCID(root,cpdest,cpChunks.chunks2[i]);
	}

	for(int i=0;i<N;i++) //send copy requests to D servers
	{
		if(msgsnd(qid3[i],&cpChunks,sizeof(cpChunks)-sizeof(long),0)==-1)
		{
			perror("msgsnd");
			exit(1);
		}
	}

	struct mtoc cpStat;
	cpStat.mtype=3;
	cpStat.status=0;
	cpStat.size=pCrawl->size;
	for(int i=0;i<pCrawl->size;i++)
	{
		cpStat.newCIDs[i]=cpChunks.chunks2[i];
	}

	if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1) //send status to C
	{
		perror("msgsnd");
		exit(1);
	}
	return chunkIDs;
}

int moveFile(struct TrieNode **root,char mvsrc[],char mvdst[])
{
	int i=0,index,len;
	struct TrieNode *pCrawl = *root;

	if(root==NULL||*root==NULL) return 1;

	while(mvsrc[i]!='\0')
	{
		index=charToInd(mvsrc[i]);
		if(!pCrawl->children[index]) return 1;
		pCrawl = pCrawl->children[index];
		i++;
	}
	len=i;
	if(pCrawl->end) return 1;
	int size = pCrawl->size;
	int *CIDs = pCrawl->CIDs;
	*root=removeK(*root,mvsrc,len,0); //deletes mvsrc key
	if(*root==NULL) *root = getNode(); //do not allow root to be null
	if(insert(*root,mvdst)) 
	{
		//dst not valid so return to previous state
		insert(*root,mvsrc);
		for(int k=0;k<size;k++)
		{
			addCID(*root,mvsrc,CIDs[k]);
		}
		return 2; 
	}
	i=0;
	pCrawl = *root;
	while(mvdst[i]!='\0')
	{
		index=charToInd(mvdst[i]);
		pCrawl = pCrawl->children[index];
		i++;
	}
	pCrawl->size =size;
	pCrawl->CIDs = CIDs;
	return 0;	
}


int checkValidPath(char filep[])
{
	int i=0;
	while(filep[i]!='\0')
	{
		if(!((filep[i]<='Z'&&filep[i]>='A')||(filep[i]<='z'&&filep[i]>='a')||(filep[i]=='/'||filep[i]=='.')||(filep[i]<='9'&&filep[i]>='0')))
			return 0;
		i++;
	}
	if(filep[i-1]=='/') return 0; 
	return 1;
}


int main()
{
	struct TrieNode *fileH = getNode(); //structure to hold file hierarchy
	int initSize=5,assigned=0;
	int *chunkIDs = (int*)malloc(sizeof(int)*initSize);
	//Create or open message queues 1 (C to M), 2 (M to C) and 6 (D to M)
	key_t key1,key2,key6;
	int qid1,qid2,qid6;
	int qid3[N];
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
	if((key6=ftok(path,6))==-1)
	{
		perror("ftok");
		exit(1);
	}
	if((qid1=msgget(key1,0))==-1)
	{
		perror("msgget");
		exit(1);
	}
	if((qid2=msgget(key2,0))==-1)
	{
		perror("msgget");
		exit(1);
	}
	if((qid6=msgget(key6,0))==-1)
	{
		perror("msgget");
		exit(1);
	}

	//get PIDs of D servers
	struct dtom getAddr;
	if(msgrcv(qid6,&getAddr,sizeof(getAddr)-sizeof(long),1,0)==-1)
	{
		perror("msgrcv");
		exit(1);
	}
	for(int i=0;i<N;i++)
	{
		pids[i]=getAddr.d_pids[i];
	}

	//M to D queues
	for(int i=0;i<N;i++)
	{
		if((qid3[i]=msgget(ftok(path,pids[i]+10),0))==-1)
		{
			perror("msgget");
			exit(1);
		}
	}

	struct ctom clientM;
	while(1){
		if(msgrcv(qid1,&clientM,sizeof(clientM)-sizeof(long),0,0)==-1)
		{
			perror("msgrcv");
			exit(1);
		}
		switch(clientM.mtype)
		{
			case 1: //Modify file hierarchy
			; //empty statement as labels can't be followed declarations
			struct mtoc sendStat;
			sendStat.mtype=1;
			if(!checkValidPath(clientM.forc))
			{
				sendStat.status=1;
				if(msgsnd(qid2,&sendStat,sizeof(sendStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			if(fileH==NULL) fileH=getNode();
			int ret;
			if(ret=insert(fileH,clientM.forc))
			{
				sendStat.status=ret;
				if(msgsnd(qid2,&sendStat,sizeof(sendStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			else
			{
				sendStat.status=0;
				if(msgsnd(qid2,&sendStat,sizeof(sendStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			break;
			case 2: //add chunk
			; //empty statement as labels can't be followed declarations
			struct mtoc dInfo;
			dInfo.mtype=2;
			strcpy(dInfo.p,path1);
			strcpy(dInfo.q,path2);
			//any 3 of N pids can be assigned here - can be randomised but in this case, N=3 so it is assigned explicitly for simplicity here
			dInfo.d1=pids[0]; 
			dInfo.d2=pids[1];
			dInfo.d3=pids[2];
			assigned++;
			if(assigned>initSize)
				chunkIDs = (int*) realloc(chunkIDs,sizeof(int)*assigned);
			chunkIDs[assigned-1] = genCID(chunkIDs,assigned-1);
			dInfo.CID = chunkIDs[assigned-1];
			//modify file hierachy to store chunk ID of corresponding file
			addCID(fileH,clientM.forc,chunkIDs[assigned-1]);
			if(msgsnd(qid2,&dInfo,sizeof(dInfo)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
			break;
			case 3: //copy
			;
			char cpsrc[100],cpdest[100];
			struct mtoc cpStat;
			cpStat.mtype=3;
			char *tokenCP,*srcf,*dstf;
			tokenCP=strtok(clientM.forc," ");
			if(tokenCP==NULL||strcmp(tokenCP,"cp")!=0)
			{
				cpStat.status=3;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			//cp source
			srcf = strtok(NULL," ");
			if(srcf==NULL)
			{
				cpStat.status=3;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			strcpy(cpsrc,srcf);
			if(checkValidPath(cpsrc)==0)
			{
				cpStat.status=2;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			if(!exist(fileH,cpsrc))
			{
				cpStat.status=1;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;

			}

			//cp destination
			dstf = strtok(NULL," ");
			if(dstf==NULL)
			{
				cpStat.status=3;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			strcpy(cpdest,dstf);
			if(checkValidPath(cpdest)==0)
			{
				cpStat.status=2;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			if(exist(fileH,cpdest))
			{
				cpStat.status=2;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			int retval=0;
			if(retval=insert(fileH,cpdest))
			{
				cpStat.status=2;
				if(msgsnd(qid2,&cpStat,sizeof(cpStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			chunkIDs=copyFile(fileH,cpsrc,cpdest,chunkIDs,&assigned,qid3,qid2);

			break;		

			case 4: //move
			;
			char mvsrc[100],mvdest[100];
			struct mtoc mvStat;
			mvStat.mtype=4;
			char *token,*src,*dst;
			token=strtok(clientM.forc," ");
			if(token==NULL||strcmp(token,"mv")!=0)
			{
				mvStat.status=3;
				if(msgsnd(qid2,&mvStat,sizeof(mvStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			//mv source
			src = strtok(NULL," ");
			if(src==NULL)
			{
				mvStat.status=3;
				if(msgsnd(qid2,&mvStat,sizeof(mvStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			strcpy(mvsrc,src);
			if(checkValidPath(mvsrc)==0)
			{
				mvStat.status=2;
				if(msgsnd(qid2,&mvStat,sizeof(mvStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			//mv destination
			dst = strtok(NULL," ");
			if(dst==NULL)
			{
				mvStat.status=3;
				if(msgsnd(qid2,&mvStat,sizeof(mvStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			strcpy(mvdest,dst);
			if(checkValidPath(mvdest)==0)
			{
				mvStat.status=2;
				if(msgsnd(qid2,&mvStat,sizeof(mvStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			int res = moveFile(&fileH,mvsrc,mvdest);
			mvStat.status=res;
			if(msgsnd(qid2,&mvStat,sizeof(mvStat)-sizeof(long),0)==-1)
			{
				perror("msgsnd");
				exit(1);
			}
			break;
			case 5: //remove
			;
			char rfile[100];
			struct mtoc rmStat;
			rmStat.mtype=5;
			char *tokenRM,*rf;
			tokenRM=strtok(clientM.forc," ");
			if(tokenRM==NULL||strcmp(tokenRM,"rm")!=0)
			{
				rmStat.status=3;
				if(msgsnd(qid2,&rmStat,sizeof(rmStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			//rm source
			rf = strtok(NULL," ");
			if(rf==NULL)
			{
				rmStat.status=3;
				if(msgsnd(qid2,&rmStat,sizeof(rmStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			strcpy(rfile,rf);
			if(checkValidPath(rfile)==0)
			{
				rmStat.status=2;
				if(msgsnd(qid2,&rmStat,sizeof(rmStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}

			if(!exist(fileH,rfile))
			{
				rmStat.status=1;
				if(msgsnd(qid2,&rmStat,sizeof(rmStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
				break;
			}
			else
			{
				rmStat.status=0;
				if(msgsnd(qid2,&rmStat,sizeof(rmStat)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}
			}
			int size=0,len=0;
			while(rfile[len++]!='\0');
			len--;
			int *CIDs = delUtil(fileH,rfile,&size);
			fileH=removeK(fileH,rfile,len,0);
			struct mtod delC;
			delC.mtype=2;
			for(int j=0;j<size;j++)
			{
				delC.chunks1[j] = CIDs[j];
			}
			delC.size1=size;
			for(int j=0;j<N;j++)
			{
				if(msgsnd(qid3[j],&delC,sizeof(delC)-sizeof(long),0)==-1)
				{
					perror("msgsnd");
					exit(1);
				}	
			}
			break;
			

		}	
	}
	

}