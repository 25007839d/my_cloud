//+------------------------------------------------------------------+
//|                                                 shanimaharaj.mq4 |
//|                                                         dushyant |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "dushyant"
#property link      "https://www.mql5.com"
#property version   "1.00"
#property strict
//OrderClose(startBuyOrder,0.01,Ask,3,Red);
// 2nd order sl value diff from triger mom and 2nd lot 1 condition removed

double x = NULL ;
double y = NULL ;
double startSellOrder_1 =NULL;
double startSellOrder_2 =NULL;
double startSellOrder_3 =NULL;
double startBuyOrder_1 = NULL;
double startBuyOrder_2 = NULL;
double startBuyOrder_3 = NULL;
double startBuySellOrder = NULL;
double startSellBuyOrder = NULL;
double b = NULL ;
double s = NULL ;
double bb = NULL ;
double ss = NULL ;
double bbb = NULL ;
double sss = NULL ;
double bs = NULL ;
double sb = NULL ;

double Total = NULL;
datetime order_time = TimeCurrent();
double buy_min_low =NULL;
double buy_max_high=NULL;
double sell_min_low =NULL;
double sell_max_high=NULL;


void CloseOrders()
{
   // Update the exchange rates before closing the orders.
   RefreshRates();
   // Log in the terminal the total of orders, current and past.
   Print(OrdersTotal());

   // Start a loop to scan all the orders.
   // The loop starts from the last order, proceeding backwards; Otherwise it would skip some orders.
   for (int i = (OrdersTotal() - 1); i >= 0; i--)
   {
      // If the order cannot be selected, throw and log an error.
      if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES) == false)
          {
         Print("ERROR - Unable to select the order - ", GetLastError());
         break;
      }

      // Create the required variables.
      // Result variable - to check if the operation is successful or not.
      bool res = false;

      // Allowed Slippage - the difference between current price and close price.
      int Slippage = 0;

      // Bid and Ask prices for the instrument of the order.
      double BidPrice = MarketInfo(OrderSymbol(), MODE_BID);
      double AskPrice = MarketInfo(OrderSymbol(), MODE_ASK);

      // Closing the order using the correct price depending on the type of order.
      if (OrderType() == OP_BUY)
          {
         res = OrderClose(OrderTicket(), OrderLots(), BidPrice, Slippage);
      }
      else if (OrderType() == OP_SELL)
          {
         res = OrderClose(OrderTicket(), OrderLots(), AskPrice, Slippage);
      }

      // If there was an error, log it.
      if (res == false) Print("ERROR - Unable to close the order - ", OrderTicket(), " - ", GetLastError());
   }
   }

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
  {
//---
double mom = iMomentum(NULL,NULL,14,PRICE_CLOSE,0);
double wpr = iWPR(NULL,0,14,0);
double macd = iMACD(NULL,0,12,26,9,PRICE_CLOSE,MODE_MAIN,0);
double bull =iBullsPower(NULL,0,13,PRICE_CLOSE,0) ;
double bear =iBearsPower(NULL,0,13,PRICE_CLOSE,0) ;
double ma21 = iMA(NULL,0,7,0,MODE_EMA,PRICE_CLOSE,0);
double ma44 = iMA(NULL,0,44,0,MODE_EMA,PRICE_CLOSE,0);
double stochastic_B = iStochastic(NULL,0,14,3,3,MODE_SMA,0,0,0);
double stochastic_R = iStochastic(NULL,0,14,3,3,MODE_SMA,0,1,0);
//trade by momentum buy and sell=================================
if  (OrdersTotal()==0 && stochastic_B>stochastic_R && stochastic_R<30 && stochastic_R>20 && s==0 && mom>100.1 && Close[1]>ma21 )


      {
         buy_min_low = Ask-3;
         buy_max_high = MathMax(High[2],High[2]);
         double startBuyOrder_1 = OrderSend(NULL,OP_BUY,0.01,Ask,12,NULL,Ask+1,NULL,0);


         //x = startBuyOrder;
         Total =  1;
         b = 1;

         Alert("by");
      }

if  (OrdersTotal()==1 && stochastic_B<stochastic_R && stochastic_R<30 && s==0 && mom<100.05 && Close[1]<ma21 )


      {

         double startBuySellOrder = OrderSend(NULL,OP_SELL,0.02,Bid,12,NULL,NULL,NULL,0);


         //x = startBuyOrder;
         Total =  1;
         bs = 1;

         Alert("slll");
      }
if (Close[0]<buy_min_low && b==1 && OrdersTotal()==1 && stochastic_B>stochastic_R && mom>100.02 && Close[1]>ma21)

     {


       double startBuyOrder_2 = OrderSend(NULL,OP_BUY,0.03,Ask,12,NULL,Ask+1,NULL,0);
       bb=1;

      // double startSellOrder_1 = OrderSend(NULL,OP_SELL,0.02,Bid,12,NULL,Bid-5,NULL,0);

     }
if (Close[0]<buy_min_low-5 && bb==1 && OrdersTotal()>1 && OrdersTotal()<2 && stochastic_B>stochastic_R && mom>100.02 && Close[1]>ma21)

     {


       double startBuyOrder_3 = OrderSend(NULL,OP_BUY,0.04,Ask,12,NULL,Ask+2,NULL,0);
       bbb=1;
       //double startSellOrder_2 = OrderSend(NULL,OP_SELL,0.03,Bid,12,NULL,Bid-5,NULL,0);

     }


if   (OrdersTotal()==0 && stochastic_B<stochastic_R && stochastic_R>70 && b==0 && mom<100.05 && Close[1]<ma21)

     {
       sell_max_high = Bid + 3;
       sell_min_low = MathMax(High[1],High[2]);
       double startSellOrder_1 = OrderSend(NULL,OP_SELL,0.01,Bid,12,NULL,Bid-1,NULL,0);

       //y = startBuyOrder;
       Total =  1;
       s = 1;

       Alert("sl");
     }

if  (OrdersTotal()==1 && stochastic_B>stochastic_R && stochastic_R>70 && stochastic_R <80 && s==0 && mom>100.1 && Close[1]>ma21 )


      {

         double startSellBuyOrder = OrderSend(NULL,OP_BUY,0.02,Ask,12,NULL,NULL,NULL,0);


         //x = startBuyOrder;
         Total =  1;
         sb = 1;

         Alert("by");
         }

if (Close[0]>sell_max_high && s==1 && OrdersTotal()==1 && stochastic_B<stochastic_R && mom<100.05 && Close[1]<ma21)

     {


       double startSellOrder_2 = OrderSend(NULL,OP_SELL,0.03,Bid,12,NULL,Bid-1,NULL,0);
       ss=1;

       //double startBuyOrder_1 = OrderSend(NULL,OP_BUY,0.02,Ask,12,NULL,Ask+5,NULL,0);

     }
if (Close[0]>sell_max_high+5 && ss==1 && OrdersTotal()>1 && OrdersTotal()<2 && stochastic_B<stochastic_R && mom<100.05 && Close[1]>ma21 )

     {


       double startSellOrder_3 = OrderSend(NULL,OP_SELL,0.04,Bid,12,NULL,Bid-2,NULL,0);
       sss=1;
      // double startBuyOrder_2 = OrderSend(NULL,OP_BUY,0.03,Ask,12,NULL,Ask+2,NULL,0);

     }


// all order close when mom 1 position match=======================


if  (b==1 && stochastic_R>75)

                     {
                         CloseOrders();
                         b = NULL;
                         bb=NULL;
                         bbb=NULL;
                     }

if  (s==1 && stochastic_R<18 )

                     {
                        CloseOrders();
                        s = NULL;
                        ss=NULL;
                        sss=NULL;

                     }


if (stochastic_B>stochastic_R && bs==1)

   {
      OrderClose(startBuySellOrder,0.02,Bid,8,Red);
      bs=0;
   }

if (stochastic_B<stochastic_R && sb==1)

   {
      OrderClose(startSellBuyOrder,0.02,Bid,8,Red);
      sb=0;
   }
 /*
if (Close[0]>ma21 && bb==1 && OrdersTotal()>1 && OrdersTotal()<4)

   {
      OrderClose(startSellOrder_1,0.01,Bid,8,Red);
      double startBuyOrder_1 = OrderSend(NULL,OP_BUY,0.04,Ask,12,NULL,Ask+3,NULL,0);
   }

if (Close[0]>ma21 && bbb==1&& OrdersTotal()>1 && OrdersTotal()<6)

   {
      OrderClose(startSellOrder_2,0.02,Bid,8,Red);
      double startBuyOrder_1 = OrderSend(NULL,OP_BUY,0.05,Ask,12,NULL,Ask+3,NULL,0);
   }

if (Close[0]<ma21 && ss==1 && OrdersTotal()>1 && OrdersTotal()<4)

   {
      OrderClose(startBuyOrder_1,0.01,Ask,8,Red);
      double startSellOrder_1 = OrderSend(NULL,OP_SELL,0.04,Bid,12,NULL,Bid-3,NULL,0);
   }

if (Close[0]<ma21 && sss==1 && OrdersTotal()>1 && OrdersTotal()<6)

   {
      OrderClose(startBuyOrder_2,0.02,Ask,8,Red);
      double startSellOrder_1 = OrderSend(NULL,OP_SELL,0.05,Bid,12,NULL,Bid-3,NULL,0);
   }
  */
}

//+------------------------------------------------------------------+
