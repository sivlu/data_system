#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>

//#endif




int main(){
    int* temp = (int*)malloc(sizeof(int)*100);
    for (int i = 0; i<10; ++i){
        temp[i] = i;
    }
    for (int i = 0; i<10; ++i) {
        printf("%d\n", temp[i]);
    }


    return 0;
}
