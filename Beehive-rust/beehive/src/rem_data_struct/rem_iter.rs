use crate::mem::RemRefTrait;
use crate::mem::SingleDerefScope;
use crate::mem::{DerefScopeBaseTrait, DerefScopeTrait};
pub trait RemIteratorTrait<'a> {
    type Item: RemRefTrait<'a>;
    fn next(&mut self, scope: &dyn DerefScopeTrait) -> Option<Self::Item>;
    fn pin(&self, scope: &dyn DerefScopeTrait);
    fn unpin(&self, scope: &dyn DerefScopeTrait);
}

pub struct RemZip1<'a, It1: RemIteratorTrait<'a>> {
    parent: Option<&'a dyn DerefScopeTrait>,
    it1: It1,
}

impl<'a, It1: RemIteratorTrait<'a>> RemZip1<'a, It1> {
    pub fn new(it1: It1, scope: &'a dyn DerefScopeTrait) -> Self {
        Self {
            parent: Some(scope),
            it1,
        }
    }
}

impl<'a, It1: RemIteratorTrait<'a>> DerefScopeBaseTrait for RemZip1<'a, It1> {
    fn pin(&self) {
        self.it1.pin(self);
    }

    fn unpin(&self) {
        self.it1.unpin(self);
    }

    fn parent(&self) -> Option<&dyn DerefScopeTrait> {
        self.parent
    }

    fn push(&self, remref: &dyn RemRefTrait) -> SingleDerefScope {
        SingleDerefScope::new(Some(self), remref)
    }
}

impl<'a, It1: RemIteratorTrait<'a>> Iterator for RemZip1<'a, It1> {
    type Item = (&'a dyn DerefScopeTrait, It1::Item);

    fn next(&mut self) -> Option<Self::Item> {
        let scope: &dyn DerefScopeTrait = self;
        let scope = scope as *const dyn DerefScopeTrait;
        match self.it1.next(unsafe { std::mem::transmute(scope) }) {
            Some(item) => Some((
                unsafe { std::mem::transmute(self as &dyn DerefScopeTrait) },
                item,
            )),
            _ => None,
        }
    }
}

pub type RemIterator<'a, It1> = RemZip1<'a, It1>;

pub struct RemZip2<'a, It1: RemIteratorTrait<'a>, It2: RemIteratorTrait<'a>> {
    parent: Option<&'a dyn DerefScopeTrait>,
    it1: It1,
    it2: It2,
}

impl<'a, It1: RemIteratorTrait<'a>, It2: RemIteratorTrait<'a>> RemZip2<'a, It1, It2> {
    pub fn new(it1: It1, it2: It2, scope: &'a dyn DerefScopeTrait) -> Self {
        Self {
            parent: Some(scope),
            it1,
            it2,
        }
    }
}

impl<'a, It1: RemIteratorTrait<'a>, It2: RemIteratorTrait<'a>> DerefScopeBaseTrait
    for RemZip2<'a, It1, It2>
{
    fn pin(&self) {
        self.it1.pin(self);
        self.it2.pin(self);
    }

    fn unpin(&self) {
        self.it1.unpin(self);
        self.it2.unpin(self);
    }

    fn parent(&self) -> Option<&dyn DerefScopeTrait> {
        self.parent
    }

    fn push(&self, remref: &dyn RemRefTrait) -> SingleDerefScope {
        SingleDerefScope::new(Some(self), remref)
    }
}

impl<'a, It1: RemIteratorTrait<'a>, It2: RemIteratorTrait<'a>> Iterator for RemZip2<'a, It1, It2> {
    type Item = (&'a dyn DerefScopeTrait, It1::Item, It2::Item);

    fn next(&mut self) -> Option<Self::Item> {
        let scope: &dyn DerefScopeTrait = self;
        let scope = scope as *const dyn DerefScopeTrait;
        match (
            self.it1.next(unsafe { std::mem::transmute(scope) }),
            self.it2.next(unsafe { std::mem::transmute(scope) }),
        ) {
            (Some(item1), Some(item2)) => Some((
                unsafe { std::mem::transmute(self as &dyn DerefScopeTrait) },
                item1,
                item2,
            )),
            _ => None,
        }
    }
}
