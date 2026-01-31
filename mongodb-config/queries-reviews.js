/* QUERY 1
Extract products liked by users similar to a target user, excluding ones that have been already
rated by the target user, and rank them by similarity weight
*/
db.reviews.aggregate([
  //Find products liked by a target user
  {
    $match: {
      "reviewer.id": "A5KIX8Y8H197J",
      score: { $gte: 4 }
    }
  },

  //Build a set with the IDs of the liked products
  {
    $group: {
      _id: null,
      liked_products: { $addToSet: "$product.id" }
    }
  },

  //Find other users who liked the same products
  {
    $lookup: {
      from: "reviews",
      let: { likedProducts: "$liked_products" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $in: ["$product.id", "$$likedProducts"] },
                { $gte: ["$score", 4] },
                { $ne: ["$reviewer.id", "A5KIX8Y8H197J"] }
              ]
            }
          }
        }
      ],
      as: "similar_reviews"
    }
  },

  { $unwind: "$similar_reviews" },

  //Compute similarity weight per user
  {
    $group: {
      _id: "$similar_reviews.reviewer.id",
      similarity_weight: { $sum: 1 },
      liked_products: { $first: "$liked_products" }
    }
  },

  //Find products liked by similar users
  {
    $lookup: {
      from: "reviews",
      let: {
        similarUser: "$_id",
        weight: "$similarity_weight"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$reviewer.id", "$$similarUser"] },
                { $gte: ["$score", 4] }
              ]
            }
          }
        },
        {
          $project: {
            product_id: "$_id",
            weight: "$$weight"
          }
        }
      ],
      as: "recommended"
    }
  },

  { $unwind: "$recommended" },

  //Remove products already liked by target user
  {
    $match: {
      $expr: {
        $not: {
          $in: ["$recommended.product_id", "$liked_products"]
        }
      }
    }
  },

  //Final aggregation
  {
    $group: {
      _id: "$recommended.product_id",
      strength: { $sum: "$recommended.weight" }
    }
  },

  { $sort: { strength: -1 } }
]);


/* QUERY 2
Compute a similarity between all pairs of reviewers based on shared high-rated products
*/
db.reviews.aggregate([
  //Find positive reviews
  {
    $match: {
      score: { $gte: 4 }
    }
  },

  //Group reviewers per product
  {
    $group: {
      _id: "$product.id",
      reviewers: { $addToSet: "$reviewer.id" }
    }
  },

  { $unwind: "$reviewers" },

  //Self-join reviewers on the same product
  {
    $lookup: {
      from: "reviews",
      let: {
        productId: "$_id",
        reviewerA: "$reviewers"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$product.id", "$$productId"] },
                { $gte: ["$score", 4] },
                { $gt: ["$reviewer.id", "$$reviewerA"] }
              ]
            }
          }
        },
        {
          $project: {
            first: "$$reviewerA",
            second: "$reviewer.id"
          }
        }
      ],
      as: "pairs"
    }
  },

  { $unwind: "$pairs" },

  //Count common high-rated products per reviewer pair
  {
    $group: {
      _id: {
        first: "$pairs.first",
        second: "$pairs.second"
      },
      similarity_score: { $sum: 1 }
    }
  },

  //Threshold
  {
    $match: {
      similarity_score: { $gt: 5 }
    }
  },

  { $sort: { similarity_score: -1 } }
]);



/* QUERY 3
Given a target user, find users who highly rated the same products of the target user in a limited time window and 
count how many distinct products have been rated in such time window.
*/
db.reviews.aggregate([
  //Find positive reviews made by a target user
  {
    $match: {
      "reviewer.id": "A5KIX8Y8H197J",
      score: { $gte: 4 }
    }
  },

  //Maintain only the product IDs + creation timestamp
  {
    $project: {
      product_id: "$product.id",
      target_time: "$created_at"
    }
  },

  //Find overlapping reviews on same product & time window (made by different users)
  {
    $lookup: {
      from: "reviews",
      let: {
        pid: "$product_id",
        ttime: "$target_time"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$product.id", "$$pid"] },
                { $gte: ["$score", 4] },
                { $ne: ["$reviewer.id", "A5KIX8Y8H197J"] },
                {
                  $lt: [
                    { $abs: { $subtract: ["$created_at", "$$ttime"] } },
                    2592000   //30 days
                  ]
                }
              ]
            }
          }
        }
      ],
      as: "overlapping_reviews"
    }
  },

  { $unwind: "$overlapping_reviews" },

  //Group (user, product) pairs
  {
    $group: {
      _id: {
        user: "$overlapping_reviews.reviewer.id",
        product: "$product_id"
      }
    }
  },

  //Count how many distinct products overlap per user
  {
    $group: {
      _id: "$_id.user",
      temporal_overlap: { $sum: 1 }
    }
  },

  //Threshold
  {
    $match: {
      temporal_overlap: { $gte: 3 }
    }
  },

  { $sort: { temporal_overlap: -1 } }
]);