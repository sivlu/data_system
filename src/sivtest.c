#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>

//#endif

void foo(char** haha){
    char buffer[100];
    strcpy(buffer, "");

    int temp[2][2] = {{-1,2},{3,4}};
    for (int i = 0; i<2; i++){
        for (int j=0; j<2; ++j){
            int cur = temp[i][j];
            char temp[20];
            sprintf(temp, "%d", cur);
            strcat(buffer,temp);
            if (j!=2-1)strcat(buffer,",");
        }
        strcat(buffer, "\n");
    }
//    printf("%s", buffer);
    *haha = strdup(buffer);

}


int main(){
    char* temp = NULL;
    foo(&temp);
    printf("%s", temp);


}
