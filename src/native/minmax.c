#include <jni.h>

JNIEXPORT jobject JNICALL Java_math_SparcceleratorCol_00024NativeMethod_00024_minmax
(JNIEnv* env, jobject input, jint buf1Off, jint buf1Len,jobject buf1, jobject resBuf)
{

  double min, max = 0;

  jbyte* _buf1;
  double *data = (*env)->GetDirectBufferAddress(env, buf1);

  double *res = (*env)->GetDirectBufferAddress(env, resBuf);


  min = data[0];
  max = data[0];
  for (int i = 1; i < buf1Len; i++) {

   if(data[i]>max)
     max = data[i];
   else if(data[i]<min)
     min = data[i];
  }


res[0] = min;
res[1] = max;
}