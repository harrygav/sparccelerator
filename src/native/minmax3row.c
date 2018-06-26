#include <jni.h>

JNIEXPORT jobject JNICALL Java_math_Sparccelerator3Row_00024NativeMethod_00024_minmax3row
(JNIEnv* env, jobject input, jint buf1Off, jint buf1Len,jobject buf1, jobject resBuf)
{

struct __attribute__((__packed__)) tuple3 {
    double first_field;
    double second_field;
    double third_field;
};

  double min1, max1, min2, max2, min3, max3 = 0.0;

  struct tuple3 *tuples;

  jbyte* _buf1;
  tuples = (*env)->GetDirectBufferAddress(env, buf1);

  double *res = (*env)->GetDirectBufferAddress(env, resBuf);


  min1 = (tuples)->first_field;
  max1 = (tuples)->first_field;

  min2 = (tuples)->second_field;
  max2 = (tuples)->second_field;

  min3 = (tuples)->third_field;
  max3 = (tuples)->third_field;

  for (int i = 1; i < buf1Len; i++) {

    double first = (tuples+i)->first_field;
    double sec = (tuples+i)->second_field;
    double third = (tuples+i)->third_field;

   if(first>max1)
     max1 = first;
   else if(first<min1)
     min1 = first;

   if(sec>max2)
     max2 = sec;
   else if(sec<min2)
     min2 = sec;

   if(third>max3)
     max3 = third;
   else if(third<min3)
     min3 = third;
  }


res[0] = min1;
res[1] = max1;
res[2] = min2;
res[3] = max2;
res[4] = min3;
res[5] = max3;
}