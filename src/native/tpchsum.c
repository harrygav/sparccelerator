#include <jni.h>

JNIEXPORT jdouble JNICALL Java_tpchsum_Sparccelerator_00024NativeMethod_00024_tpchsum
(JNIEnv* env, jobject input, jobject buf1, jint buf1Off, jint buf1Len)
{

struct __attribute__((__packed__)) tuple5 {
    double first_field;
    double second_field;
    double third_field;
    double fourth_field;
    double fifth_field;
};
struct tuple5 *tuples;

int i,n,j;
jbyte* _buf1;

double sum = 0;
tuples = (*env)->GetDirectBufferAddress(env, buf1);

for (i = 0; i < buf1Len; i++)
{
    // sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum+= ((tuples+i)->first_field * (1- (tuples+i)->second_field) * (1+ (tuples+i)->third_field));
}

return sum;

}