using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

namespace NaiadControlFlowMicrobenchmark
{
	class MainClass
	{

        public static void DoIt(string[] args, Controller c) {

            int numSteps = int.Parse(args[0]);
			int numElements = int.Parse(args[1]);

            using (var computation = c.NewComputation())
            {

                //Console.WriteLine ("computation.Configuration.ProcessID: " + computation.Configuration.ProcessID);

                /*
                Stream<int, Epoch> i0 = null;
                if (computation.Configuration.ProcessID == 0) {
                    i0 = new[] { 1 }.AsNaiadStream (computation);
                } else {
                    i0 = Enumerable.Empty<int>().AsNaiadStream (computation);
                }

                i0.Iterate ((lc, i) => {

                    return i.Select(x => x + 1).Where(x => x <= numSteps);

                    //return i.Select(x => {
                    //  Console.WriteLine(x);
                    //  return x + 1;
                    //}).Where(x => x <= numSteps);

                },
                    Int32.MaxValue,
                    "Iteration");
                */




                

                Stream<int, Epoch> coll0 = null;
                if (c.Configuration.ProcessID == 0)
                {
                    var l = new List<int>();
                    for (int i = 0; i < numElements; i++)
                    {
                        l.Add(i);
                    }
                    coll0 = l.AsNaiadStream(computation);
                }
                else
                {
                    coll0 = Enumerable.Empty<int>().AsNaiadStream(computation);
                }

                coll0 = coll0.Select(x => x).PartitionBy(x => x).Select(x => x);

                coll0.Iterate((lc, coll) =>
                {

                    //return coll.Select(x => x + 1);

                    return coll.Select(x => x + 1).Synchronize(x => true);

                    /*return coll.Select(x =>
                    {
                        Console.WriteLine("ProcID: " + c.Configuration.ProcessID + ", num: " + x);
                        return x + 1;
                    });*/

                },
                    numSteps - 1,
                    "Iteration");


                computation.Activate();
                computation.Join();

                if (c.Configuration.ProcessID == 0)
                {
                    Console.WriteLine("Warmup run finished");
                }
            }
        }

		public static void Main (string[] args)
		{

            using (var c = NewController.FromArgs(ref args))
            {
                for (int i = 0; i < 5; i++)
                {
					var wsw = Stopwatch.StartNew();

                    DoIt(args, c);

					wsw.Stop();
					Console.WriteLine("Warmup measurement: " + wsw.ElapsedMilliseconds);

                    Console.WriteLine("Sleeping");
                    Thread.Sleep(100);
                }

                Console.WriteLine("Starting measurement");
                var sw = Stopwatch.StartNew();
                DoIt(args, c);
                sw.Stop();
				if (c.Configuration.ProcessID == 0) {
					Console.WriteLine ("Elapsed: " + sw.ElapsedMilliseconds);
				}

                c.Join();

				if (c.Configuration.ProcessID == 0)
				{
					Console.WriteLine("Computation finished");
				}
            }
		}
	}
}
