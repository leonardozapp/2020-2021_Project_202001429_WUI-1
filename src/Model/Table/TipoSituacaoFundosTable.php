<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoSituacaoFundos Model
 *
 * @property \App\Model\Table\SituacaoFundosTable&\Cake\ORM\Association\HasMany $SituacaoFundos
 *
 * @method \App\Model\Entity\TipoSituacaoFundo newEmptyEntity()
 * @method \App\Model\Entity\TipoSituacaoFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoSituacaoFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoSituacaoFundosTable extends Table
{
    /**
     * Initialize method
     *
     * @param array $config The configuration for the Table.
     * @return void
     */
    public function initialize(array $config): void
    {
        parent::initialize($config);

        $this->setTable('tipo_situacao_fundos');
        $this->setDisplayField('situacao');
        $this->setPrimaryKey('id');

        $this->hasMany('SituacaoFundos', [
            'foreignKey' => 'tipo_situacao_fundo_id',
        ]);
    }

    /**
     * Default validation rules.
     *
     * @param \Cake\Validation\Validator $validator Validator instance.
     * @return \Cake\Validation\Validator
     */
    public function validationDefault(Validator $validator): Validator
    {
        $validator
            ->integer('id')
            ->allowEmptyString('id', null, 'create');

        $validator
            ->scalar('situacao')
            ->maxLength('situacao', 40)
            ->requirePresence('situacao', 'create')
            ->notEmptyString('situacao');

        return $validator;
    }
}
