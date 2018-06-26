#include <jni.h>

JNIEXPORT jobject JNICALL Java_tpchsum_Sparccelerator_00024NativeMethod_00024_tpchsumtuple2test
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
//start

// Get the class we wish to return an instance of
    jclass clazz = (*env)->FindClass(env, "scala/Tuple2");

    // Get the method id of an empty constructor in clazz
    jmethodID constructor = (*env)->GetMethodID(env, clazz, "<init>", "()V");


//end
int i,n,j;
jbyte* _buf1;

double sum = 0;
tuples = (*env)->GetDirectBufferAddress(env, buf1);

for (i = 0; i < buf1Len; i++)
{
    // sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum+= ((tuples+i)->first_field * (1- (tuples+i)->second_field) * (1+ (tuples+i)->third_field));

}
// Create an instance of clazz
    jobject obj = (*env)->NewObject(env, clazz, constructor);

    // Get Field references
    jfieldID param1Field = (*env)->GetFieldID(env, clazz, "_1", "D");
    jfieldID param2Field = (*env)->GetFieldID(env, clazz, "_2", "D");

    // Set fields for object
    (*env)->SetDoubleField(env, obj, param1Field, sum);
    (*env)->SetDoubleField(env, obj, param2Field, 0);
// return object
    return obj;

}