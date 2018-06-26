#include <jni.h>

JNIEXPORT jobject JNICALL Java_tpchsum_SparcceleratorCol_00024NativeMethod_00024_tpchsumcol
(JNIEnv* env, jobject input, jint buf1Off, jint buf1Len,jobject buf1, jobject buf2,jobject buf3,jobject buf4,jobject buf5, jobject resBuf)
{

  double sum, sum1, sum2 = 0;

  jbyte* _buf1;
  double *data = (*env)->GetDirectBufferAddress(env, buf1);

  jbyte* _buf2;
  double *data2 = (*env)->GetDirectBufferAddress(env, buf2);

  jbyte* _buf3;
  double *data3 = (*env)->GetDirectBufferAddress(env, buf3);

  jbyte* _buf4;
  double *data4 = (*env)->GetDirectBufferAddress(env, buf4);

  jbyte* _buf5;
  double *data5 = (*env)->GetDirectBufferAddress(env, buf5);

  double *res = (*env)->GetDirectBufferAddress(env, resBuf);


  for (int i = 0; i < buf1Len; i++) {

    //sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum+= (data[i] * (1- data2[i]) * (1+ data3[i]));

    //sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum1+= (data[i] * (1- data2[i]));

    //sum(l_quantity) as sum_qty,
    sum2+= data[i];
  }


res[0] = sum;
res[1] = sum1;
res[2] = sum2;

}