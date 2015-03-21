/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.tkvs.userclient;

/**
 *
 * @author sachin
 */
public class MyKey
  {

    int k;

    public MyKey(int i)
      {
        k = i;
      }

    public int getKey()
      {
        return k;
      }

    public String toString()
      {
        return String.valueOf(k);
      }

  }
