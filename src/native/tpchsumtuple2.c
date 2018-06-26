#include <jni.h>

JNIEXPORT jobject JNICALL Java_tpchsum_Sparccelerator_00024NativeMethod_00024_tpchsumtuple2
(JNIEnv* env, jobject input, jobject buf1, jint buf1Off, jint buf1Len, jobject resBuf)
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
double sum1 = 0;
double sum2 = 0;


tuples = (*env)->GetDirectBufferAddress(env, buf1);

double *res = (*env)->GetDirectBufferAddress(env, resBuf);

for (i = 0; i < buf1Len; i++)
{
    // sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum+= ((tuples+i)->first_field * (1- (tuples+i)->second_field) * (1+ (tuples+i)->third_field));

    //sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum1+= ((tuples+i)->first_field * (1- (tuples+i)->second_field));

    //sum(l_quantity) as sum_qty,
    sum2+= (tuples+i)->first_field;

}

res[0] = sum;
res[1] = sum1;
res[2] = sum2;


}