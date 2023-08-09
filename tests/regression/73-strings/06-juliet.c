// PARAM: --disable ana.base.limit-string-addresses --enable ana.int.interval --enable ana.base.arrays.nullbytes

#include <goblint.h>
#include <string.h>
#include <stdlib.h>

// TODO: tackle memset -> map it to for loop with set for each cell

int main() {
    CWE121_Stack_Based_Buffer_Overflow__src_char_declare_cpy_01_bad();
    CWE126_Buffer_Overread__CWE170_char_loop_01_bad();
    CWE126_Buffer_Overread__CWE170_char_strncpy_01_bad();
    CWE126_Buffer_Overread__char_declare_loop_01_bad();
    CWE571_Expression_Always_True__string_equals_01_bad();
    CWE665_Improper_Initialization__char_cat_01_bad();
    CWE665_Improper_Initialization__char_ncat_11_bad();
    
    return 0;
}

void CWE121_Stack_Based_Buffer_Overflow__src_char_declare_cpy_01_bad()
{
    char * data;
    char dataBuffer[100];
    data = dataBuffer;
    /* FLAW: Initialize data as a large buffer that is larger than the small buffer used in the sink */
    memset(data, 'A', 100-1); /* fill with 'A's */
    data[100-1] = '\0'; /* null terminate */
    {
        char dest[50] = "";
        /* POTENTIAL FLAW: Possible buffer overflow if data is larger than dest */
        strcpy(dest, data); // WARN
    }
}

void CWE126_Buffer_Overread__CWE170_char_loop_01_bad()
{
    {
        char src[150], dest[100];
        int i;
        /* Initialize src */
        memset(src, 'A', 149);
        src[149] = '\0';
        for(i=0; i < 99; i++)
        {
            dest[i] = src[i];
        }
        /* FLAW: do not explicitly null terminate dest after the loop */
        __goblint_check(dest[42] != '\0');
        __goblint_check(dest[99] != '\0'); // UNKNOWN
    }
}

void CWE126_Buffer_Overread__CWE170_char_strncpy_01_bad()
{
    {
        char data[150], dest[100];
        /* Initialize data */
        memset(data, 'A', 149);
        data[149] = '\0';
        /* strncpy() does not null terminate if the string in the src buffer is larger than
         * the number of characters being copied to the dest buffer */
        strncpy(dest, data, 99); // WARN
        /* FLAW: do not explicitly null terminate dest after the use of strncpy() */
    }
}

void CWE126_Buffer_Overread__char_declare_loop_01_bad()
{
    char * data;
    char dataBadBuffer[50];
    char dataGoodBuffer[100];
    memset(dataBadBuffer, 'A', 50-1); /* fill with 'A's */
    dataBadBuffer[50-1] = '\0'; /* null terminate */
    memset(dataGoodBuffer, 'A', 100-1); /* fill with 'A's */
    dataGoodBuffer[100-1] = '\0'; /* null terminate */
    /* FLAW: Set data pointer to a small buffer */
    data = dataBadBuffer;
    {
        size_t i, destLen;
        char dest[100];
        memset(dest, 'C', 100-1);
        dest[100-1] = '\0'; /* null terminate */
        destLen = strlen(dest);
        __goblint_check(destLen == 99);
        /* POTENTIAL FLAW: using length of the dest where data
         * could be smaller than dest causing buffer overread */
        for (i = 0; i < destLen; i++)
        {
            dest[i] = data[i];
        }
        dest[100-1] = '\0';
    }
}

void CWE665_Improper_Initialization__char_cat_01_bad()
{
    char * data;
    char dataBuffer[100];
    data = dataBuffer;
    /* FLAW: Do not initialize data */
    ; /* empty statement needed for some flow variants */
    {
        char source[100];
        memset(source, 'C', 100-1); /* fill with 'C's */
        source[100-1] = '\0'; /* null terminate */
        /* POTENTIAL FLAW: If data is not initialized properly, strcat() may not function correctly */
        strcat(data, source); // WARN
    }
}

void CWE571_Expression_Always_True__string_equals_01_bad() 
{
    char charString[10] = "true";
    int cmp = strcmp(charString, "true");
    __goblint_check(cmp == 0); // UNKNOWN

    /* FLAW: This expression is always true */
    if (cmp == 0) 
    {
        printf("always prints");
    }
}

void CWE665_Improper_Initialization__char_ncat_11_bad()
{
    char * data;
    char dataBuffer[100];
    data = dataBuffer;
    if(rand())
    {
        /* FLAW: Do not initialize data */
        ; /* empty statement needed for some flow variants */
    }
    {
        size_t sourceLen;
        char source[100];
        memset(source, 'C', 100-1); /* fill with 'C's */
        source[100-1] = '\0'; /* null terminate */
        sourceLen = strlen(source);
        __goblint_check(sourceLen == 99);
        /* POTENTIAL FLAW: If data is not initialized properly, strncat() may not function correctly */
        strncat(data, source, sourceLen); // WARN --> why not?? spurious
    }
}
