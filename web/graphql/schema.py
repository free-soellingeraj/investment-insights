"""Strawberry GraphQL schema and Litestar integration."""

import strawberry
from strawberry.litestar import make_graphql_controller

from .resolvers import Query

schema = strawberry.Schema(query=Query)

GraphQLController = make_graphql_controller(
    schema,
    path="/graphql",
)
