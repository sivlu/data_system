#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>

//#endif



int main(){
    char temp[] = "as.d.a.s";
    char* token = strtok(temp, ".");
    char* t1 = strtok(NULL, "");

//    printf("%d %d %d %d", token==NULL, t1==NULL, t2==NULL, t3==NULL);
    printf("%s\n%s\n", token, t1);

}
