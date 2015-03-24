from collections import deque
from dask.core import istask, subs


def head(task):
    """Return the top level node of a task"""

    if task is None:
        return None
    if istask(task):
        return task[0]
    elif isinstance(task, list):
        return list
    else:
        return task


def args(task):
    """Get the arguments for the current task"""

    if istask(task):
        return task[1:]
    elif isinstance(task, list):
        return task
    else:
        return ()


class Traverser(object):
    """Traverser interface for tasks"""

    def __init__(self, term, stack=None):
        self._term = term
        if not stack:
            self._stack = deque([None])
        else:
            self._stack = stack

    def copy(self):
        return Traverser(self._term, deque(self._stack))

    def next(self):
        subterms = args(self._term)
        if not subterms:
            # No subterms, pop off stack
            self._term = self._stack.pop()
        else:
            self._term = subterms[0]
            for t in reversed(subterms[1:]):
                self._stack.append(t)

    @property
    def current(self):
        return head(self._term)

    def skip(self):
        self._term = self._stack.pop()


def preorder_traversal(task):
    """Traverse a task."""
    for item in task:
        if isinstance(item, tuple):
            for i in preorder_traversal(item):
                yield i
        elif isinstance(item, list):
            yield list
            for i in preorder_traversal(item):
                yield i
        else:
            yield item


class Var(object):
    """A variable object. Matches with any node."""

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self._name


# A variable to represent *all* variables in a discrimination net
VAR = Var('?')


class Node(tuple):
    """A Discrimination Net node."""

    __slots__ = ()

    def __new__(cls, edges=None, patterns=None):
        edges = edges if edges else {}
        patterns = patterns if patterns else []
        return tuple.__new__(cls, (edges, patterns))

    @property
    def edges(self):
        """A dictionary, where the keys are edges, and the values are nodes"""
        return self[0]

    @property
    def patterns(self):
        """A list of all patterns that currently match at this node"""
        return self[1]


class RewriteRule(object):
    """A rewrite rule.

    Expresses ``lhs`` -> ``rhs``, for variables ``vars``.

    Parameters
    ----------

    lhs : task
        The left-hand-side of the rewrite rule.
    rhs : task
        The right-hand-side of the rewrite rule.
    vars: tuple
        Tuple of variables found in the lhs.
    """

    def __init__(self, lhs, rhs, vars=()):
        if not istask(lhs) or not istask(rhs):
            raise TypeError("lhs and rhs must both be tasks")
        if not isinstance(vars, tuple):
            raise TypeError("vars must be a tuple of variables")
        self.lhs = lhs
        self.rhs = rhs
        self._varlist = [t for t in preorder_traversal(lhs) if t in vars]
        # Reduce vars down to just variables found in lhs
        self.vars = tuple(set(self._varlist))

    def __str__(self):
        return "RewriteRule({0}, {1}, {2})".format(self.lhs, self.rhs, self.vars)

    def __repr__(self):
        return str(self)


class RuleSet(object):
    """A set of rewrite rules.

    Forms a structure for fast rewriting over a set of rewrite rules.

    Parameters:
    -----------
    rules: RewriteRule
        One or instances of RewriteRule
    """

    def __init__(self, *rules):
        self._net = Node()
        self._rules = []
        for p in rules:
            self.add(p)

    def add(self, rule):
        """Add a rule to the RuleSet."""

        if not isinstance(rule, RewriteRule):
            raise TypeError("rule must be instance of RewriteRule")
        vars = rule.vars
        curr_node = self._net
        ind = len(self._rules)
        # List of variables, in order they appear in the POT of the term
        for t in preorder_traversal(rule.lhs):
            prev_node = curr_node
            if t in vars:
                t = VAR
            if t in curr_node.edges:
                curr_node = curr_node.edges[t]
            else:
                curr_node.edges[t] = Node()
                curr_node = curr_node.edges[t]
        # We've reached a leaf node. Add the term index to this leaf.
        prev_node.edges[t].patterns.append(ind)
        self._rules.append(rule)

    def iter_matches(self, term):
        """Lazily find matchings for term from the RuleSet.

        Paramters:
        ----------
        term : task

        Returns:
        --------
        A generator that yields tuples of ``(rule, subs)``, where ``rule`` is
        the rewrite rule being matched, and ``subs`` is a dictionary mapping
        the variables in that lhs of the rule to their matching values in the
        term."""

        S = Traverser(term)
        for m, syms in _match(S, self._net):
            for i in m:
                rule = self._rules[i]
                subs = _process_match(rule, syms)
                if subs is not None:
                    yield rule, subs

    def _rewrite(self, term):
        """Apply the rewrite rules in RuleSet to top level of term"""

        try:
            rule, sd = next(self.iter_matches(term))
            term = rule.rhs
            for key, val in sd.items():
                term = subs(term, key, val)
            return term
        except StopIteration:
            return term

    def rewrite(self, term, strategy="bottom_up"):
        """Apply the rule set to ``term``.

        Parameters:
        -----------
        term: dask, or a task
            The item to be rewritten
        strategy: str, optional
            The rewriting strategy to use. Options are "bottom_up" (default),
            or "top_level".
        """
        func = strategies[strategy]
        if isinstance(term, dict):
            return dict((k, func(self, v)) for (k, v) in term.items())
        return func(self, term)


def _top_level(net, term):
    return net._rewrite(term)


def _bottom_up(net, term):
    if not istask(term):
        return net._rewrite(term)
    else:
        new_args = tuple(_bottom_up(net, t) for t in args(term))
        new_term = (head(term),) + new_args
    return net._rewrite(new_term)


strategies = {'top_level': _top_level,
              'bottom_up': _bottom_up}


def _match(S, N):
    """Structural matching of term S to discrimination net node N."""

    stack = deque()
    matches = deque()
    restore_state_flag = False
    while True:
        n = N.edges.get(VAR, None)
        if n and restore_state_flag:
            matches.pop()
        if n and not restore_state_flag:
            stack.append((S.copy(), N))
            matches.append(S._term)
            S.skip()
            N = n
        else:
            n = N.edges.get(S.current, None)
            if n:
                restore_state_flag = False
                N = n
                S.next()
                continue
            if S.current is None:
                yield N.patterns, matches
            try:
                (S, N) = stack.pop()
                restore_state_flag = True
            except:
                return


def _process_match(rule, syms):
    """Process a match to determine if it is correct, and to find the correct
    substitution that will convert the term into the pattern.

    Parameters:
    -----------
    rule : RewriteRule
    syms : iterable
        Iterable of subterms that match a corresponding variable.

    Returns:
    --------
    A dictionary of {vars : subterms} describing the substitution to make the
    pattern equivalent with the term. Returns ``None`` if the match is
    invalid."""

    subs = {}
    varlist = rule._varlist
    if not len(varlist) == len(syms):
        print(rule, syms, varlist)
        assert 0 == 1
    for v, s in zip(varlist, syms):
        if v in subs and subs[v] is not s:
            return None
        else:
            subs[v] = s
    return subs
