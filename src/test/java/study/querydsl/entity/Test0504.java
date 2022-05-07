package study.querydsl.entity;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Projections;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import study.querydsl.Dto.MemberDto;
import study.querydsl.Dto.QMemberDto;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;

import java.util.List;

import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
@Rollback(value = false)
public class Test0504 {

    @PersistenceContext
    EntityManager em;

    JPAQueryFactory jpaQueryFactory;

    @BeforeEach
    public void before() throws Exception {
        jpaQueryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

        em.flush();
        em.clear();
    }

    /**
     * member1를 찾아라.
     * @throws Exception
     */
    @Test
    public void startJPQL() throws Exception {
        Member result = em.createQuery("select m from Member m where m.username =:username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();
        Assertions.assertThat(result.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() throws Exception {
        Member member = jpaQueryFactory
                .selectFrom(QMember.member)
                .where(QMember.member.username.eq("member1"))
                .fetchOne();
        Assertions.assertThat(member.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() throws Exception {
        Member member = jpaQueryFactory
                .selectFrom(QMember.member)
                .where(QMember.member.username.eq("member1"),
                        QMember.member.team.name.eq("teamA"))
                .fetchOne();
//        for (Member fetch1 : fetch) {
//            System.out.println("member = " + fetch1.getUsername());
//        }
        Assertions.assertThat(member.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() throws Exception {
        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .fetch();
        Member member = jpaQueryFactory
                .selectFrom(QMember.member)
                .fetchFirst();
        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(QMember.member)
                .fetchResults();
        List<Member> teamA = jpaQueryFactory
                .select(QMember.member)
                .from(QMember.member, team)
                .where(QMember.member.team.name.eq("teamA"))
                .fetch();
        System.out.println(teamA);
        System.out.println("getTotal = " + results.getTotal());
        List<Member> results1 = results.getResults();
        System.out.println("results1 = " + results1);
    }

    /**
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 오름차순(asc)
     * 단 2에서 회원이름이 없으면 마지막에 출력(nulls Last)
     * @throws Exception
     */
    @Test
    public void sort() throws Exception {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();
        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member member_null = result.get(2);

        Assertions.assertThat(member5.getUsername()).isEqualTo("member5");
        Assertions.assertThat(member6.getUsername()).isEqualTo("member6");
        Assertions.assertThat(member_null.getUsername()).isNull();
    }
    
    @Test
    public void paging() throws Exception {
        QueryResults<Member> result = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(3)
                .fetchResults();

        Assertions.assertThat(result.getTotal()).isEqualTo(4);
        Assertions.assertThat(result.getLimit()).isEqualTo(3);
        Assertions.assertThat(result.getOffset()).isEqualTo(1);
        Assertions.assertThat(result.getResults().size()).isEqualTo(3);
    }

    @Test
    public void aggregation() throws Exception {
        List<Tuple> fetch = jpaQueryFactory
                .select(
                        member.age.count(),
                        member.age.sum()
                )
                .from(member)
                .fetch();
        Tuple tuple = fetch.get(0);
        AssertionsForClassTypes.assertThat(tuple.get(member.age.count())).isEqualTo(4);
        System.out.println("age_count = " + member.age.count());
        System.out.println("age_sum = " + member.age.sum());
    }

    /**
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void teamABelongsAllMem() throws Exception {
        List<Member> results = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        for (Member result : results) {
            System.out.println("results = " + result);
        }
    }

    /**
     * 회원과 팀을 조인하면서, 팀 이름이 teamA 팀만 조인, 회원은 모두 조회
     * JPQL: select m,
     */
    @Test
    public void join_on_filtering() throws Exception {
        List<Tuple> teamA = jpaQueryFactory
                .select(member, team)
                .from(member)
                .join(member.team, team).on(team.name.eq(("teamA")))
                .fetch();
        for (Tuple tuple : teamA) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void join_on_relation() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> fetch = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();
        for (Tuple tuple : fetch) {
            System.out.println("tuple = " + tuple);
        }

    }

    @Test
    public void fetch_join() throws Exception {
        em.flush();
        em.clear();

        Member member = jpaQueryFactory
                .selectFrom(QMember.member)
                .join(QMember.member.team, team).fetchJoin()
                .where(QMember.member.username.eq("member1"))
                .fetchOne();
        System.out.println(member);
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                        )
                )
                .fetch();
    }

    /**
     * 나이가 평균 이상인 회원 조회
     * @throws Exception
     */
    @Test
    public void subQueryGoe() throws Exception {
        QMember memberSub = new QMember("memberSub");
        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();
        Assertions.assertThat(fetch).extracting("age")
                .containsExactly(30, 40);
    }

    @Test
    public void findDtoBySetter() throws Exception {
        List<MemberDto> fetch = jpaQueryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : fetch) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findDtoByQueryProjection() throws Exception {
        List<MemberDto> fetch = jpaQueryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : fetch) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception {
        String usernameParam = "member1";
        Integer ageParam = 10;
        
        List<Member> result = searchMember1(usernameParam, ageParam);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }
        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return jpaQueryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }


}
