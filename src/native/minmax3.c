#include <jni.h>

JNIEXPORT jobject JNICALL Java_math_Sparccelerator3Col_00024NativeMethod_00024_minmax3
(JNIEnv* env, jobject input, jint buf1Off, jint buf1Len,jobject buf1,jobject buf2,jobject buf3, jobject resBuf)
{

  double min1, max1, min2, max2, min3, max3 = 0.0;

  jbyte* _buf1;
  double *data = (*env)->GetDirectBufferAddress(env, buf1);

  jbyte* _buf2;
  double *data2 = (*env)->GetDirectBufferAddress(env, buf2);

  jbyte* _buf3;
  double *data3 = (*env)->GetDirectBufferAddress(env, buf3);

  double *res = (*env)->GetDirectBufferAddress(env, resBuf);


  min1 = data[0];
  max1 = data[0];

  min2 = data2[0];
  max2 = data2[0];

  min3 = data3[0];
  max3 = data3[0];

  for (int i = 1; i < buf1Len; i++) {

   if(data[i]>max1)
     max1 = data[i];
   else if(data[i]<min1)
     min1 = data[i];

   if(data2[i]>max2)
     max2 = data2[i];
   else if(data2[i]<min2)
     min2 = data2[i];

   if(data3[i]>max3)
     max3 = data3[i];
   else if(data3[i]<min3)
     min3 = data3[i];
  }


res[0] = min1;
res[1] = max1;
res[2] = min2;
res[3] = max2;
res[4] = min3;
res[5] = max3;
}