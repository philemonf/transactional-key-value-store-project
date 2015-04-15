/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;

/**
 *
 * @author sachin
 */
public class MVCC2PL implements Algorithm
  {

    @Override
    public ReadResponse read(ReadRequest r)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    @Override
    public GenericSuccessResponse write(WriteRequest r)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    @Override
    public GenericSuccessResponse begin(BeginRequest r)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    @Override
    public GenericSuccessResponse commit(CommitRequest r)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    private class Transaction
      {
          int transactionId;
          
      }

  }
