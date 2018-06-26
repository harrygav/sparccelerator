#include <jni.h>
#include <math.h>

double distance(int py, int px){
     //return sqrt((float)((px)*(px)-(py)*(py)));
     //val dist = Math.sqrt(Math.pow(pointX - pointY, 2))
     return sqrt(pow(px - py, 2));

}
double distance2(int py, int px, int py1, int px1){
     //return sqrt((float)((px)*(px)-(py)*(py)));
     //val dist = Math.sqrt(Math.pow(pointX - pointY, 2))
     return sqrt(pow(px - py, 2)+pow(px1 - py1, 2));

}
JNIEXPORT jobject JNICALL Java_euclidean_Sparccelerator_00024NativeMethod_00024_euclidean
(JNIEnv* env, jobject input, jobject buf1, jint buf1Off, jint buf1Len, jobject resbuf)
{

struct __attribute__((__packed__)) tuple5 {
    int first_field;
    int second_field;
    int third_field;
    int fourth_field;
    int fifth_field;
};
struct tuple5 *tuples;

int i,n,j;
jbyte* _buf1;


tuples = (*env)->GetDirectBufferAddress(env, buf1);

double *res = (*env)->GetDirectBufferAddress(env, resbuf);

for (i = 0; i < buf1Len; i++)
{
    res[i] = distance2((tuples+i)->first_field,(tuples+i)->second_field, (tuples+i)->third_field, (tuples+i)->fourth_field);
}

}