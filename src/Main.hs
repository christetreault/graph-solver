{-|
Module : Main
Description : Graph Solver
License : GPL-3
Stability : probably not very

This module is a simple graph solver. Nodes can have dependencies, and use
a "solver" function to "solve" them, once their dependencies are satisfied.
-}

module Main where

import qualified Data.Map as M
import Data.Graph
import Data.List
import Data.Maybe
import Control.Monad

-- conSolveFn
import Control.Concurrent

-- parSolveFn
import Control.Parallel.Strategies

-- Imports for testing
import Criterion.Main
import System.Random
import System.Process

-- | A triple of a node, its key, and the keys of its dependencies
type Node node = (node, Int, [Int])

-- | The solver input
data Input node soln =
   Input {solver :: node -> [soln] -> IO soln,
          getNode :: Vertex -> Node node,
          getVertex :: Int -> Maybe Vertex,
          graph :: Graph}

-- | Construct an Input
input :: (node ->[soln] -> IO soln) -- ^ The solver function.
         -> [Node node] -- ^ A node, a unique identifier for the node
         -- and a list of node IDs upon which this node depends.
         -> Input node soln -- ^ A new input object
input s g =
   Input {solver = s,
          getNode = getter,
          getVertex = vGetter,
          graph = inGraph}
   where (inGraph, getter, vGetter) = graphFromEdges g

-- | Gets a node from the graph by key, and the nodes it depends on
getVal :: Input node soln -- ^ The graph to access
          -> Int -- ^ The key corresponding to the node
          -> Maybe (node, [Int]) -- ^ A pair of a node, and the keys
          -- of its dependencies, or Nothing if the node isn't in the graph
getVal i k = do
   vertex <- getVertex i k
   (node, _, deps) <- return $ getNode i vertex
   return (node, deps)

-- | The final output of the solver. A map from keys to solutions
type Output soln = M.Map Int soln

-- | Get a list of nodes that are ready to be solved
readyNodes :: Input node soln -- ^ The input
              -> Output soln -- ^ The current built up solution map
              -> [Node node] -- ^ A list of nodes who's dependencies
              -- have all been solved
readyNodes i o = filter readyNode nkdList
   where
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

-- | Solve the input and produce an output map. Solves batches of ready
-- nodes using the batch solver function
solve :: Input node soln -- ^ The input to solve
         -> ([Node node]
             -> Output soln
             -> (node -> [soln] -> IO soln)
             -> IO (Output soln)) -- ^ A function to solve a batch of nodes
         -> IO (Output soln) -- ^ An IO action containing the solutions
solve i f = solve' M.empty
   where
      solve' o' = do
         nodes <- return $ readyNodes i o'
         if null nodes
            then
            return o'
            else
            do
               o'' <- f nodes o' (solver i)
               solve' o''

-- | Attempts to solve a node. Fails if the provided solution map isn't complete
trySolve :: Node node -- ^ The node to try
            -> Maybe [soln] -- ^ Potentially a list of solutions for this node
            -> (node -> [soln] -> IO soln) -- ^ The solver function
            -> IO (Maybe (Int, soln)) -- ^ A solution, and its key, or Nothing
            -- if the prerequisite solution list was incomplete
trySolve _ Nothing _ = return Nothing
trySolve (n, k, _) (Just s) f = do
   soln <- f n s
   return $ Just (k, soln)

-- | Adds a list of key/solution pairs to a solution map
addAll :: Output soln -- ^ The solution map
          -> [(Int, soln)] -- ^ The new solutions to add
          -> Output soln -- ^ A new solution map with solutions added
addAll o [] = o
addAll o ((k, s):xs) = addAll (M.insert k s o) xs

-- | Solves a list of nodes serially
serSolveFn :: [Node node] -- ^ The nodes to solve
              -> Output soln -- ^ The current built-up solution map
              -> (node -> [soln] -> IO soln) -- ^ The solver function
              -> IO (Output soln) -- ^ The updated solution map
serSolveFn [] o _ = return o
serSolveFn n o f = do
   mRes <- mapM (\a -> trySolve a (getSolutions a o) f) n
   res <- return $ map fromJust $ filter isJust mRes
   return $ addAll o res

-- | Solves a list of nodes in parallel using Control.Parallel
parSolveFn :: (NFData soln)
              => [Node node] -- ^ The nodes to solve
              -> Output soln -- ^ The current built-up solution map
              -> (node -> [soln] -> IO soln) -- ^ The solver function
              -> IO (Output soln) -- ^ The updated solution map
parSolveFn [] o _ = return o
parSolveFn n o f = do
   mRes <- mapM mapTrySolve n
   res <- return $ map fromJust $ filter isJust mRes
   return $ addAll o res
   where
      mapTrySolve a = runEval $ rseq $ trySolve a (getSolutions a o) f

data ToThread node soln = Destroy
                        | Solve (Node node) (Maybe [soln])

data FromThread soln = ThreadDeath
                     | Result (IO (Maybe (Int, soln)))

-- | Solves a list of nodes in parallel using Control.Concurrent
conSolveFn :: Int -- ^ The number of threads to spawn
              -> [Node node] -- ^ The nodes to solve
              -> Output soln -- ^ The current built-up solution map
              -> (node -> [soln] -> IO soln) -- ^ The solver function
              -> IO (Output soln) -- ^ The updated solution map
conSolveFn _ [] o _ = return o
conSolveFn nt n o f = do
   tt <- newChan
   ft <- newChan
   taList <- return $ replicate nt $ threadAction tt ft
   mapM_ forkIO taList
   solveList <- return $ map toToThread n
   writeList2Chan tt solveList
   writeList2Chan tt $ replicate nt Destroy
   running nt [] ft
   where
      running 0 resList _ = do
         mRes <- mapM (\(Result r) -> r) resList
         res <- return $ map fromJust $ filter isJust mRes
         return $ addAll o res
      running c resList ft' = do
         curr <- readChan ft'
         case curr of
            ThreadDeath -> running (c - 1) resList ft'
            result -> running c (result:resList) ft'
      toToThread n' = Solve n' (getSolutions n' o)
      threadAction tt' ft' = do
         val <- readChan tt'
         case val of
            Destroy -> writeChan ft' ThreadDeath
            (Solve n' s) -> do
               writeChan ft' $ Result $! trySolve n' s f
               threadAction tt' ft'





-- | main test harness
main :: IO ()
main = do
   threadCount <- getNumCapabilities
   defaultMain [bgroup "dmesg"
                [bgroup "2x4"
                 [bench "serial" $ nfIO
                  $ solve (testInput 2 4) serSolveFn,
                  bench "parallel" $ nfIO
                  $ solve (testInput 2 4) parSolveFn,
                  bench "concurrent" $ nfIO
                  $ solve (testInput 2 4) (conSolveFn threadCount)],
                 bgroup "8x4"
                 [bench "serial" $ nfIO
                  $ solve (testInput 8 4) serSolveFn,
                  bench "parallel" $ nfIO
                  $ solve (testInput 8 4) parSolveFn,
                  bench "concurrent" $ nfIO
                  $ solve (testInput 8 4) (conSolveFn threadCount)],
                 bgroup "16x4"
                 [bench "serial" $ nfIO
                  $ solve (testInput 16 4) serSolveFn,
                  bench "parallel" $ nfIO
                  $ solve (testInput 16 4) parSolveFn,
                  bench "concurrent" $ nfIO
                  $ solve (testInput 16 4) (conSolveFn threadCount)]

                 ]]
               {-[bgroup "Randoms"
                [bgroup "2X2"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 2 2) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 2 2) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 2 2) (conSolveFn threadCount)],
                 bgroup "4X4"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 4 4) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 4 4) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 4 4) (conSolveFn threadCount)],
                 bgroup "8X8"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 8 8) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 8 8) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 8 8) (conSolveFn threadCount)],
                 bgroup "16X16"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 16 16) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 16 16) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 16 16) (conSolveFn threadCount)],
                 bgroup "32X32"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 32 32) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 32 32) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 32 32) (conSolveFn threadCount)],
                 bgroup "64X64"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 64 64) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 64 64) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 64 64) (conSolveFn threadCount)],
                 bgroup "128X128"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 128 128) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 128 128) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 128 128) (conSolveFn threadCount)],
                 bgroup "256X256"
                 [bench "Serial" $ nfIO
                  $ solve (rTestInput 256 256) serSolveFn,
                  bench "Parallel" $ nfIO
                  $ solve (rTestInput 256 256) parSolveFn,
                  bench "Concurrent" $ nfIO
                  $ solve (rTestInput 256 256) (conSolveFn threadCount)]]]-}
   where
      testData w d = testGraph w d (mkStdGen 800815) nFn_dmesg
      testInput w d = input sFn_dmesg $ testData w d
      --testData w d = testGraph w d (mkStdGen 1337) nFn_randoms --fibs
      --rTestInput w d = input sFn_randoms $ testData w d


-- | Test data

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

nFn_dmesg :: Int -> Int -> [Int] -> Node Int
nFn_dmesg = nFn_fibs

sFn_dmesg :: Int -> [Int] -> IO Int
sFn_dmesg = sFn_ioStuff $ do
   out <- readProcess "dmesg" [] []
   return $ length out

sFn_ioStuff :: IO Int -> Int -> [Int] -> IO Int
sFn_ioStuff a = foldM goDoStuff
   where
      goDoStuff 0 0 = goDoStuff 1 1
      goDoStuff lhs 0 = goDoStuff lhs 1
      goDoStuff 0 rhs = goDoStuff 1 rhs
      goDoStuff lhs rhs = do
         ioRes <- a
         return $ (ioRes `mod` lhs) + (ioRes `mod` rhs)

nFn_fibs :: Int -> Int -> [Int] -> Node Int
nFn_fibs _ k [] = (0, k, [])
nFn_fibs n k d = ((abs $ n `mod` length d), k, d)

nFn_randoms :: Int -> Int -> [Int] -> Node Int
nFn_randoms = nFn_fibs

sFn_randoms :: Int -> [Int] -> IO Int
sFn_randoms = foldM goRand
   where
      goRand 0 0 = goRand 1 1
      goRand lhs 0 = goRand lhs 1
      goRand 0 rhs = goRand 1 rhs
      goRand lhs rhs = do
         lhs' <- randomIO
         lhs'' <- randomIO
         rhs' <- randomIO
         rhs'' <- randomIO
         return $ (lhs' + lhs'' `mod` lhs) + (rhs' + rhs'' `mod` rhs)

sFn_badFibs :: Int -> [Int] -> IO Int
sFn_badFibs n s = return $ foldl' goFibs n s
   where
      goFibs lhs rhs = badFibs $ (lhs + rhs) `mod` 35

sFn_goodFibs :: Int -> [Int] -> IO Int
sFn_goodFibs n s = return $ foldl' goFibs n s
   where
      goFibs lhs rhs = goodFibs $ (lhs + rhs) `mod` 35

-- | Bad fibs implementation
badFibs :: Int -> Int
badFibs 0 = 0
badFibs 1 = 1
badFibs n = badFibs (n - 1) + badFibs (n - 2)

-- | Good fibs implementation
goodFibs :: Int -> Int
goodFibs 0 = 0
goodFibs 1 = 1
goodFibs n = goodFibs' (n, 2, 1, 0)
   where
      goodFibs' (to, count, n, nmo)
         | to == count = n + nmo
         | otherwise = goodFibs' (to, (count + 1), (n + nmo), n)
