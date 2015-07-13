import Criterion.Main
import Control.Concurrent
import System.Process
import Control.Monad
import Control.Parallel.Strategies
import qualified Data.Map as M
import Data.Graph
import Data.List
import System.Random

-- TYPES

data Workers a b =
   Workers
   {toThread :: Chan a,
    fromThread :: Chan b}

-- | A triple of a node, its key, and the keys of its dependencies
type Node node = (node, Int, [Int])

-- | The solver input
data Input node soln =
   Input {getNode :: Vertex -> Node node,
          getVertex :: Int -> Maybe Vertex,
          graph :: Graph}

-- | Construct an Input
input :: [Node node] -- ^ A node, a unique identifier for the node
         -- and a list of node IDs upon which this node depends.
         -> Input node soln -- ^ A new input object
input g =
   Input {getNode = getter,
          getVertex = vGetter,
          graph = inGraph}
   where (inGraph, getter, vGetter) = graphFromEdges g

-- | The final output of the solver. A map from keys to solutions
type Output soln = M.Map Int soln

-- SOLVING FUNCTIONS

-- | Get a list of nodes that are ready to be solved
readyNodes :: Input node soln -- ^ The input
              -> Output soln -- ^ The current built up solution map
              -> [(Node node, [soln])] -- ^ A list of nodes who's dependencies
              -- have all been solved, paired with their solutions
readyNodes i o = map fromJust'
                 $ filter dropNothing
                 $ map pairSoln
                 $ filter readyNode nkdList
   where
      fromJust' (n, Just s) = (n, s)
      dropNothing (_, Nothing) = False
      dropNothing _ = True
      pairSoln n = (n, getSolutions n o)
      verts = vertices (graph i)
      nkdList = map (getNode i) verts
      readyNode (_, k', d') =
         M.notMember k' o
         && not (any (`M.notMember` o) d')

-- | Get the solutions required to solve this node
getSolutions :: Node node -- ^ The node in question
                -> Output soln -- ^ The current built up solution map
                -> Maybe [soln] -- ^ A list of solutions, or Nothing if
                -- there is an unsolved dependency
getSolutions (_, _, d) o = do
   mSolns <- return $ map (`M.lookup` o) d
   sequence mSolns

-- | Adds a list of key/solution pairs to a solution map
addAll :: Output soln -- ^ The solution map
          -> [(Int, soln)] -- ^ The new solutions to add
          -> Output soln -- ^ A new solution map with solutions added
addAll o [] = o
addAll o ((k, s):xs) = addAll (M.insert k s o) xs

initWorkers :: (Workers a b -> IO ()) -> Int -> IO (Workers a b)
initWorkers a n = do
   tx <- newChan
   rx <- newChan
   let workers = Workers {toThread = tx, fromThread = rx}
   let actions =  replicate n $ a workers
   mapM_ forkIO actions
   return workers

--action :: (Int, Int) -> IO (Int, Float)
action :: (Node Int, [Float]) -> IO (Int, Float)
action ((n, k, _), s) = do
   let plusOrMinusFive = (0.01 :: Float) * fromIntegral (4 + (n `mod` 3))
   _ <- system $ "sleep " ++ show plusOrMinusFive
   let useSolns = (foldl' (+) 0 s)
   return $ (k, plusOrMinusFive + useSolns)

--workerAction :: Workers (Int, Int) (Int, Float) -> IO ()
workerAction w = forever $ do
   arg <- readChan $ toThread w
   res <- action arg
   writeChan (fromThread w) res

--serSolve :: [(Int, Int)] -> IO [(Int, Float)]
serSolve = mapM action

--parSolve :: Workers (Int, Int) (Int, Float) -> [(Int, Int)] -> IO [(Int, Float)]
parSolve :: Workers (Node node, [soln]) (Int, soln)
            -> [(Node node, [soln])]
            -> IO [(Int, soln)]
parSolve w i = do
   writeList2Chan (toThread w) i
   waitForAll (length i) []
   where
      waitForAll 0 o = sequence o
      waitForAll n o = waitForAll (n - 1) (readChan (fromThread w) : o)

solve :: ([(Node node, [soln])] -> IO [(Int, soln)])
         -> Input node soln
         -> IO (Output soln)
solve f i = solve' M.empty
   where
      solve' o = do
         nodes <- return $ readyNodes i o
         if null nodes
            then
            return o
            else
            do
               o' <- f nodes
               solve' $ addAll o o'

main = do
   let rand = mkStdGen 1337
   let testTen =
          runEval $ evalList rdeepseq $ testGraph 10 10 rand nFn_sleep
   let testHundred =
          runEval $ evalList rdeepseq $ testGraph 100 10 rand nFn_sleep
   tc <- getNumCapabilities
   workers <- initWorkers workerAction tc
   defaultMain [bgroup "10"
                [bench "serial" $ nfIO $ (solve serSolve) (input testTen),
                 bench "parallel"
                 $ nfIO $ (solve $ parSolve workers) (input testTen)],
                bgroup "100"
                [bench "serial" $ nfIO $ (solve serSolve) (input testHundred),
                 bench "parallel"
                 $ nfIO $ (solve $ parSolve workers) (input testHundred)]]

-- TEST FUNCTIONS

nFn_sleep :: Int -> Int -> [Int] -> Node Int
nFn_sleep n k d = (5 + (n `mod` 3), k, d)

-- | Generates a test graph. This graph will have the form of a grid of w by d
-- nodes. The top row will have no dependencies. For all other rows, each node
-- will depend on all nodes of the row above it.
testGraph :: (RandomGen gen)
             => Int -- ^ The "width" of the test input. I.E., how many
             -- nodes should be solvable in parallel. Must be > 0
             -> Int -- ^ The depth of the test input. I.E., how many
             -- levels of nodes there are. Must be > 0
             -> gen -- ^ A random number generator
             -> (Int -> Int -> [Int] -> Node node) -- ^ A function that takes a
             -- random number, a key, and a list of dependencies, and produces a
             -- node
             -> [Node node]
testGraph w d g nFn = nodeList
   where
      lhsList = take (d * w) $ randoms g :: [Int]
      midList = [0..(w * d)]
      rhsList = join $ replicate w [] : init (map widthList intervalList)
      intervalList = (*) <$> [0..(d - 1)] <*> [w]
      widthList n = replicate w [n..(n + (w - 1))]
      nodeList = zipWith3 nFn lhsList midList rhsList
