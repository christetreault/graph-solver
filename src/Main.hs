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
import Data.Time.Clock
import Control.Parallel.Strategies
import Data.Maybe
import Control.Monad

-- Imports for testing
import Criterion.Main
import System.Random

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
   vertex <- (getVertex i) k
   (node, _, deps) <- return $ (getNode i) vertex
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
      toNode = map (\(n', k', d') -> n')
      readyNode (n', k', d') =
         (filter (\d'' -> M.notMember d'' o) d') == []
         && M.notMember k' o

-- | Get the solutions required to solve this node
getSolutions :: Node node -- ^ The node in question
                -> Output soln -- ^ The current built up solution map
                -> Maybe [soln] -- ^ A list of solutions, or Nothing if
                -- there is an unsolved dependency
getSolutions (_, _, d) o = do
   mSolns <- return $ map ((flip M.lookup) o) d
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
         if (null nodes)
            then
            return o'
            else
            do
               o'' <- f nodes o' (solver i)
               solve' o''

-- | Attempts to solve a node. Fails if the provided solution map isn't complete
trySolve :: Node node -- ^ The node to try
            -> Output soln -- ^ The current built up solution map
            -> Maybe [soln] -- ^ Potentially a list of solutions for this node
            -> (node -> [soln] -> IO soln) -- ^ The solver function
            -> IO (Maybe (Int, soln)) -- ^ A solution, and its key, or Nothing
            -- if the prerequisite solution list was incomplete
trySolve _ o Nothing _ = return Nothing
trySolve (n, k, _) o (Just s) f = do
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
   mRes <- mapM (\a -> trySolve a o (getSolutions a o) f) n
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
   mRes <- mapM (\a -> trySolve a o (getSolutions a o) f) n
   res <- return $ map fromJust $ filter isJust mRes
   return $ addAll o (runEval $ rdeepseq `parList` res)

-- | main test harness
main :: IO ()
main =
   defaultMain [bgroup "Serial"
                [bgroup "Good Fibs"
                 [bench "2X2" $ nfIO $ solve (gfTestInput 2 2) serSolveFn,
                  bench "4X4" $ nfIO $ solve (gfTestInput 4 4) serSolveFn],
                 -- bench "8X8" $ nfIO $ solve (gfTestInput 8 8) serSolveFn],
                 bgroup "Bad Fibs"
                 [bench "2X2" $ nfIO $ solve (bfTestInput 2 2) serSolveFn,
                  bench "4X4" $ nfIO $ solve (bfTestInput 4 4) serSolveFn]],
                 -- bench "8X8" $ nfIO $ solve (bfTestInput 8 8) serSolveFn]],
                bgroup "Parallel"
                [bgroup "Good Fibs"
                 [bench "2X2" $ nfIO $ solve (gfTestInput 2 2) parSolveFn,
                  bench "4X4" $ nfIO $ solve (gfTestInput 4 4) parSolveFn],
                 -- bench "8X8" $ nfIO $ solve (gfTestInput 8 8) parSolveFn],
                 bgroup "Bad Fibs"
                 [bench "2X2" $ nfIO $ solve (bfTestInput 2 2) parSolveFn,
                  bench "4X4" $ nfIO $ solve (bfTestInput 4 4) parSolveFn]]]
                 -- bench "8X8" $ nfIO $ solve (bfTestInput 8 8) parSolveFn]]]
   where
      testData w d = testGraph w d (mkStdGen 1337) nFn_fibs
      gfTestInput w d = input sFn_goodFibs $ testData w d
      bfTestInput w d = input sFn_badFibs $ testData w d


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

nFn_fibs :: Int -> Int -> [Int] -> Node Int
nFn_fibs _ k [] = (0, k, [])
nFn_fibs n k d = ((abs $ n `mod` length d), k, d)

sFn_badFibs :: Int -> [Int] -> IO Int
sFn_badFibs n s = return $ foldl' goFibs n s
   where
      goFibs lhs rhs = badFibs (lhs + rhs)

sFn_goodFibs :: Int -> [Int] -> IO Int
sFn_goodFibs n s = return $ foldl' goFibs n s
   where
      goFibs lhs rhs = goodFibs (lhs + rhs)

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
