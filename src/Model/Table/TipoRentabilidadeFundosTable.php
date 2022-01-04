<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoRentabilidadeFundos Model
 *
 * @property \App\Model\Table\CadastroFundosTable&\Cake\ORM\Association\HasMany $CadastroFundos
 *
 * @method \App\Model\Entity\TipoRentabilidadeFundo newEmptyEntity()
 * @method \App\Model\Entity\TipoRentabilidadeFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoRentabilidadeFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoRentabilidadeFundosTable extends Table
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

        $this->setTable('tipo_rentabilidade_fundos');
        $this->setDisplayField('rentabilidade');
        $this->setPrimaryKey('id');

        $this->hasMany('CadastroFundos', [
            'foreignKey' => 'tipo_rentabilidade_fundo_id',
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
            ->scalar('rentabilidade')
            ->maxLength('rentabilidade', 100)
            ->requirePresence('rentabilidade', 'create')
            ->notEmptyString('rentabilidade');

        return $validator;
    }
}
