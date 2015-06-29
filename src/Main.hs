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

import Criterion.Main

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
   defaultMain [bench "serial" $ nfIO $ solve testInput serSolveFn,
                bench "parallel" $ nfIO $ solve testInput parSolveFn]



   {-
   startSer <- getCurrentTime
   putStrLn $ "Solving with serial solver:"
   serResult <- solve testInput serSolveFn
   putStrLn $ "Serial done: " ++ [last (show serResult)]
   endSer <- getCurrentTime

   startPar <- getCurrentTime
   putStrLn $ "Solving with parallel solver:"
   parResult <- solve testInput parSolveFn
   putStrLn $ "Parallel done: " ++ [last(show parResult)]
   endPar <- getCurrentTime

   putStrLn $ show (endSer `diffUTCTime` startSer) ++ " elapsed (Serial)."
   putStrLn $ show (endPar `diffUTCTime` startPar) ++ " elapsed (Parallel)."
-}

-- | Test data
testInput :: Input Integer Integer
testInput = input fn gr
   where
      gr = [(0, 0, [1, 2]),
            (1, 1, [4, 5]), (2, 2, [5, 6]),
            (4, 4, [7, 8]), (5, 5, [7, 8]), (6, 6, [7, 8]),
            (7, 7, [9]), (8, 8, [9]),
            (9, 9, [])]
      fn node solns = return $ foldl' expensive node solns
         where
            expensive lhs rhs = (fact (lhs `mod` 100) (lhs `mod` 100)) + rhs

-- | Factorial function. Used by testInput
fact 0 acc = acc
fact 1 acc = acc
fact n acc = fact (n - 1) (acc * (n - 1))
